/*
 * Pony SQL Database ( http://www.ponysql.ru/ )
 * Copyright (C) 2019-2020 IllayDevel.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pony.database;

import java.io.*;
import java.util.Arrays;

import com.pony.util.ByteArrayUtil;
import com.pony.util.IntegerVector;
import com.pony.util.UserTerminal;
import com.pony.util.Cache;
import com.pony.debug.*;

/**
 * A file format that allows for the very quick retreival of data that is
 * stored within it.  To allow for such quick reference of information in the
 * file, we must make stipulations about how the data is stored.
 * <ol>
 * <li> Each data element in the table must be a fixed length.  This could be
 *   thought of like a 'sector' of a disk drive.  Or in a table, each row may
 *   be of the fixed size.
 * <li> We keep track of deleted rows via a linked list of sectors.
 * </ol>
 * The header of the data store is as follows:
 * <p><pre>
 *   0 4 (int)  : MAGIC        - used to identify file type.
 *   4 4 (int)  : version      - version of the file store.
 *   8 4 (int)  : sector_size  - the size of each sector in the store.
 *  12 8 (long) : delete_head  - the head sector of the delete list.
 *  20 8 (long) : sectors_used - number of sectors being used (not deleted).
 *  28 1 (byte) : open         - set to 1 when file opened, 0 when closed.
 *  29 4 (int)  : sector_start - offset where sector information starts.
 *  33 ...  63                 - reserved
 *  64 ... 191                 - reserved buffer for misc. state data.
 * 192 ... sector_start        - reserved
 * </pre><p>
 * Each sector contains a 5 byte header.  This header includes a byte that
 * contains either USED or DELETED, and a int pointer to the next chained
 * sector.  The int pointer is used to either represent a pointer to the next
 * sector in the chain of USED sectors, with -1 indicating the end.  Or, if
 * the sector is DELETED, it points to the next deleted sector in the chain.
 *
 * @author Tobias Downer
 */

public final class FixedSizeDataStore {

    /**
     * The Magic number used to help identify that the file we are reading is
     * formatted as a fixed size data store.
     */
    private final static int MAGIC = 0x0badbead;

    /**
     * The offset in the file where the sector data starts.
     */
    private final static int SECTOR_DATA_OFFSET = 512;

    /**
     * The number of bytes that are stored in a sector in addition to the
     * user data in a sector.
     */
    private final static int EXTRA_SECTOR_SIZE = 5;

    /**
     * The mark that indicates whether a sector is deleted (available) or being
     * used.
     */
    private final static byte USED = 0,
            DELETED = (byte) 0x080;

    /**
     * If true then sectors are cached in the 'sector_cache'.
     */
    private final static boolean SECTORS_CACHED = true;


    /**
     * A DebugLogger object we can use to write debug messages to.
     */
    private final DebugLogger debug;

    /**
     * The size of each 'sector'
     */
    private int sector_size;

    /**
     * The File that keeps the data.
     */
    private final File data_file;

    /**
     * The RandomAccessFile object for the data file.
     */
    private RandomAccessFile data_store;

    /**
     * Set to true if we opened the store in read only mode, otherwise false.
     */
    private boolean read_only;

    /**
     * The file size of the data store.
     */
    private long data_store_size;

    /**
     * The offset where the header information finishes and the sector data
     * starts.
     */
    private int sector_offset;

    /**
     * The sector buffer.  This is filled with the information in some given
     * sector.
     */
    private byte[] sector_buffer;

    /**
     * The sector that we currently have loaded into the buffer.
     */
    private int buffered_sector;

    /**
     * The head of the deleted sectors.
     */
    private int delete_head;

    /**
     * The number of used sectors in the store.
     */
    private int used_sector_count;

    /**
     * The number of locks that have been put on this store.  If this number is
     * > 0 then we may not reclaim deleted sector because another thread may be
     * reading the data.
     */
    private int lock_count;

    /**
     * A cache of sectors read from the store.
     */
    private final Cache sector_cache;


    /**
     * Constructs the data store.  If 'sector_size' <= 0 then we determine
     * sector size when the file is opened.  If cached_access is true then
     * all access to the store is through a cache which will greatly improve
     * the performance of read dominated access.
     */
    public FixedSizeDataStore(File data_file, int sector_size,
                              boolean cache_access,
                              DebugLogger logger) {
        this.debug = logger;

        // Enable the cache?
        if (cache_access) {
            sector_cache = new Cache(64);
        } else {
            sector_cache = null;
        }

        if (sector_size > 0) {
            this.sector_size = sector_size + EXTRA_SECTOR_SIZE;
        } else {
            this.sector_size = -1;
        }
        this.data_file = data_file;
    }

    public FixedSizeDataStore(File data_file, int sector_size,
                              DebugLogger logger) {
        this(data_file, sector_size, SECTORS_CACHED, logger);
    }

    // ---------- Internal methods ----------

    /**
     * Returns true if the store is locked from reclaiming deleted rows.
     */
    boolean locked() {
        return (lock_count > 0);
    }

    /**
     * Returns the total number of sectors in the file.
     */
    private int sectorCount() throws IOException {
        // PROFILE: data_store.length() is expensive so we keep the length as an
        //   instance variable
//    return (int) ((data_store.length() - sector_offset) / sector_size);
        return (int) ((data_store_size - sector_offset) / sector_size);
    }

    /**
     * Seeks to the 'nth' sector in the store.
     */
    private long seekSector(int sector) throws IOException {
        long ra_index = (sector * sector_size);
        long seek_to = ra_index + sector_offset;
        data_store.seek(seek_to);  // Skip past the header.
        return seek_to;
    }

    /**
     * Read the 'nth' sector from the store and fills the internal
     * 'sector_buffer' with the contents.
     */
    private void readSector(int sector) throws IOException {

        // If the buffered sector is already loaded then don't re-read.
        if (buffered_sector != sector) {

            if (sector_cache != null) {
                // If this sector is in the cache then use the cached entry instead.
                Integer cacheKey = new Integer(sector);
                byte[] sbuf = (byte[]) sector_cache.get(cacheKey);
                if (sbuf == null) {
                    // If not in the cache then read from the file.
                    seekSector(sector);
                    data_store.readFully(sector_buffer, 0, sector_size);
                    sbuf = new byte[sector_size];
                    System.arraycopy(sector_buffer, 0, sbuf, 0, sector_size);
                    sector_cache.put(cacheKey, sbuf);
                } else {
                    // Otherwise, read the cached entry.
                    System.arraycopy(sbuf, 0, sector_buffer, 0, sector_size);
                }
            } else {
                // If no caching then read the sector
                seekSector(sector);
                data_store.readFully(sector_buffer, 0, sector_size);
            }

            buffered_sector = sector;
        }
    }

    /**
     * Sets the length of the data store to the given size.  This has a side-
     * effect of setting the file pointer to the end of the file.
     */
    private void setDataStoreSize(long new_size) throws IOException {
        long p = new_size - 1;
        if (p > 0) {
            data_store.seek(p);
            data_store.write(0);
            data_store_size = new_size;
        }
    }

    /**
     * Writes the sector data in 'sector_buffer' to the given sector offset in
     * the store.
     */
    private void writeSector(int sector, int length) throws IOException {
        long seek_to = seekSector(sector);
        // If writing to end of file, extend the file size.
        if (seek_to == data_store_size) {
            // This will extend the file size by 16 sector lengths and add the
            // additional sectors to the deleted chain.
            setDataStoreSize(seek_to + sector_size);
            seekSector(sector);
        }
        // Check just to make sure,
        if (length <= sector_size) {
            data_store.write(sector_buffer, 0, length);
            if (sector_cache != null) {
                // Copy this into the cache.
                byte[] sbuf = new byte[sector_size];
                System.arraycopy(sector_buffer, 0, sbuf, 0, length);
                sector_cache.put(new Integer(sector), sbuf);
            }
        } else {
            throw new IOException("length > sector_size");
        }
    }

    /**
     * Writes the sector data in 'sector_buffer' to the given sector offset in
     * the store.
     */
    private void writeSector(int sector) throws IOException {
        writeSector(sector, sector_size);
    }

    /**
     * Sets up the sector header information in 'sector_buffer'.
     */
    private void setSectorHeader(byte status, int next_sector)
            throws IOException {
        sector_buffer[0] = status;
        sector_buffer[1] = (byte) ((next_sector >>> 24) & 0xFF);
        sector_buffer[2] = (byte) ((next_sector >>> 16) & 0xFF);
        sector_buffer[3] = (byte) ((next_sector >>> 8) & 0xFF);
        sector_buffer[4] = (byte) ((next_sector >>> 0) & 0xFF);
    }

    /**
     * Writes the contents of the byte[] array to the sector, setting the
     * USED flag to true, and the 'next' int in the sector header.
     * <p>
     * NOTE: Assumes length is less than user space size of sector.
     */
    private int writeBufToSector(int sector, int next_sector,
                                 byte[] buf, int offset, int length) throws IOException {

        // Write a new sector buffer entry,
        setSectorHeader(USED, next_sector);

        System.arraycopy(buf, offset, sector_buffer, 5, length);

        // NOTE: Notice the order here.  We update header state first, then
        //  write the sector.  If a crash happens between 'synch' and 'writeSector'
        //  all that will happen is we'll be left with a hanging chain.  There
        //  should be no corruption.

        // Add 1 to the used sector count.
        ++used_sector_count;
        // Sync the file
        synch();
        // Write the sector in the buffer
        writeSector(sector, length + 5);
        // We now have this sector in the buffer
        buffered_sector = sector;

        // Return the sector we wrote this to,
        return sector;
    }

    /**
     * Reclaims the first sector from the free sector list.
     */
    private int reclaimTopFree() throws IOException {
        // There's a sector we can use so use it!
        int free_sector = delete_head;
        // Take this sector out of the chain of deleted records.
        readSector(free_sector);

        int c1 = (((int) sector_buffer[1]) & 0x0FF);
        int c2 = (((int) sector_buffer[2]) & 0x0FF);
        int c3 = (((int) sector_buffer[3]) & 0x0FF);
        int c4 = (((int) sector_buffer[4]) & 0x0FF);

        delete_head = (c1 << 24) + (c2 << 16) + (c3 << 8) + (c4);
        return free_sector;
    }

    /**
     * Finds the first free available sector that we can use.  If we are
     * reclaiming from the deleted list, the deleted row is taken from the
     * linked list immediately.
     * <p>
     * NOTE: This method may alter 'delete_head' changing the list of deleted
     *   sectors.
     */
    private int findFreeSector() throws IOException {
        // Are we locked and can we reclaim a deleted sector?
        if (!locked() && delete_head != -1) {
            return reclaimTopFree();
        }

        // Either we are locked or there are no deleted sectors in the chain.
        // The new sector is at the end of the store.
        return sectorCount();

    }

    /**
     * Finds the first free available sector past the next one.  This means,
     * if locked or delete_head == -1 then we return the sectorCount() + 1,
     * otherwise we reclaim the next available of the delete queue.
     */
    private int findFreeSectorPastNext() throws IOException {
        // Are we locked and can we reclaim a deleted sector?
        if (!locked() && delete_head != -1) {
            return reclaimTopFree();
        }

        // Either we are locked or there are no deleted sectors in the chain.
        // The new sector is at the end of the store.
        return sectorCount() + 1;
    }

    /**
     * Finds the first 'n' available sectors that we can use.  If we are
     * reclaiming from the deleted list, the deleted row(s) are taken from the
     * linked list immediately.
     * <p>
     * NOTE: This method may alter 'delete_head' changing the list of deleted
     *   sectors.
     */
    private int[] findFreeSectors(int count) throws IOException {
        int fs_index = 0;
        int[] free_sectors = new int[count];
        // Are we locked, and can we reclaim a deleted sector?
        if (!locked()) {
            while (fs_index < count && delete_head != -1) {
                free_sectors[fs_index] = reclaimTopFree();
                ++fs_index;
            }
        }
        int sec_count = sectorCount();
        // Free are on end of file now,
        while (fs_index < count) {
            free_sectors[fs_index] = sec_count;
            ++sec_count;
            ++fs_index;
        }
        // Return the top list of free sectors.
        return free_sectors;
    }


    // ---------- Public methods ----------

    /**
     * Returns the size of the data store file.  This is the total number of
     * bytes stored in the data store.
     */
    public long totalSize() {
        return data_file.length();
    }

    /**
     * Every data store has a 128 byte buffer that can be used to store state
     * information.  The buffer starts at offset 64 of the file until offset 192.
     * This method writes data to that offset.
     */
    public void writeReservedBuffer(byte[] info, int offset, int length,
                                    int res_offset) throws IOException {
        if ((length + res_offset) > 128) {
            throw new Error("Attempted to write > 128 bytes in reserve buffer.");
        }
        data_store.seek(res_offset + 64);
        data_store.write(info, offset, length);
    }

    public void writeReservedBuffer(byte[] info, int offset, int length)
            throws IOException {
        writeReservedBuffer(info, offset, length, 0);
    }

    /**
     * Reads from the buffer reserve into the given byte array.
     */
    public void readReservedBuffer(byte[] info, int offset, int length)
            throws IOException {
        if (length > 128) {
            throw new Error("Attempted to read > 128 bytes from reserve buffer.");
        }
        data_store.seek(64);
        data_store.readFully(info, offset, length);
    }

    // Byte array used to synchronize data in store.
    // Enough room for two longs.
    private final byte[] sync_buffer = new byte[16];

    /**
     * Synchronizes the memory store with the file header.  This writes
     * information into the header.  This should be called periodically.
     * Synch does nothing for a read only store.
     */
    public void synch() throws IOException {
        if (!read_only) {
            // Write the head deleted sector.
            ByteArrayUtil.setLong(delete_head, sync_buffer, 0);
            // Write the number of sectors that are used (not deleted).
            ByteArrayUtil.setLong(used_sector_count, sync_buffer, 8);

            // Write the update
            // Skip past magic int and sector size int
            data_store.seek(12);
            data_store.write(sync_buffer, 0, 16);
        }
    }

    /**
     * Performs a hard synchronization of this store.  This will force the OS
     * to synchronize the contents of the data store.  hardSynch does nothing
     * for a read only store.
     */
    public void hardSynch() throws IOException {
        if (!read_only) {
            synch();
            // Make sure we are also synchronized in the file system.
            try {
                data_store.getFD().sync();
            } catch (SyncFailedException e) { /* ignore */ }
        }
    }

    /**
     * Returns true if the store has been opened in read only mode.
     */
    public boolean isReadOnly() {
        return read_only;
    }

    /**
     * Opens the data store.  The data store can be opened in 'read only' mode.
     * Returns 'true' if the open procedure should repair itself (dirty open) or
     * false if the file was cleanly closed down.
     * <p>
     * It is not possible to open a damaged store in read only mode.
     *
     * @param read_only if true, then the database is opened in read only mode,
     *   otherwise it is opened in read/write mode.
     */
    public boolean open(boolean read_only) throws IOException {
        this.read_only = read_only;

        // If the file doesn't exist, check we have a valid sector size.
        if (!data_file.exists()) {
            if (sector_size <= 0) {
                throw new IOException("Sector size not set for new file.");
            }
        }

        // Open the file
        String mode = read_only ? "r" : "rw";
        data_store = new RandomAccessFile(data_file, mode);
        data_store.seek(0);
        // Does the header exist?
        if (data_store.length() < SECTOR_DATA_OFFSET) {
            if (read_only) {
                throw new IOException(
                        "Unable to open FixedSizeDataStore.  No header found.");
            }

            ByteArrayOutputStream bout =
                    new ByteArrayOutputStream(SECTOR_DATA_OFFSET);
            DataOutputStream dout = new DataOutputStream(bout);

            // No, so create the header
            // Write the magic int
            dout.writeInt(MAGIC);
            // The version of the file type.
            dout.writeInt(0x0100);
            // Write the sector size
            dout.writeInt(sector_size);
            // Write the delete_head
            dout.writeLong(-1);
            // Write the number of sectors that are being used.
            dout.writeLong(0);
            // Write whether file open or closed
            dout.writeByte(0);
            // Write the offset where the sector information starts.
            dout.writeInt(SECTOR_DATA_OFFSET);

            // Transfer to a new buffer and write entirely to file.
            byte[] buf = bout.toByteArray();
            dout.close();
            byte[] buf2 = new byte[SECTOR_DATA_OFFSET];
            System.arraycopy(buf, 0, buf2, 0, buf.length);
            for (int i = buf.length; i < SECTOR_DATA_OFFSET; ++i) {
                buf2[i] = (byte) 255;
            }
            data_store.write(buf2);

        }
        data_store.seek(0);
        // Set the size of the file.
        data_store_size = data_store.length();

        // Read the header,
        if (data_store.readInt() == MAGIC) {
            // Read the version number,
            int version = data_store.readInt();
            if (version != 0x0100) {
                throw new IOException("Unknown version.");
            }
            // Check the sector size is right,
            int ss_check = data_store.readInt();
            // If sector_size not set yet, then set it from value in file.
            if (sector_size <= 0) {
                sector_size = ss_check;
            }
            if (ss_check == sector_size) {

                boolean need_repair = false;

                // Find the head of the deleted sectors linked list.
                delete_head = (int) data_store.readLong();
                // Find the number of sectors that are being used.
                used_sector_count = (int) data_store.readLong();
                // Did we close down cleanly?
                need_repair = data_store.readByte() != 0;
                // The offset where the sector data starts.
                sector_offset = data_store.readInt();

                sector_buffer = new byte[sector_size];
                buffered_sector = -2;

                // Write the 'open' flag to indicate store is open
                // ( Only if we opening in read/write mode )
                if (!read_only) {
                    data_store.seek(28);
                    data_store.writeByte(1);
                }

                // Check sector count * sector_size + sector_offset is the same size
                // as the data store.
                int pred_size = (sectorCount() * sector_size) + sector_offset;
//        System.out.println("Sector Count: " + sectorCount());
//        System.out.println("Sector Size: " + sector_size);
//        System.out.println("Sector Offset: " + sector_offset);
//        System.out.println("Data Store Size: " + data_store_size);
//        System.out.println("Pred Size: " + pred_size);

                if (pred_size != data_store_size) {
                    debug.write(Lvl.ERROR, this,
                            "The FixedSizeDataStore file size is incorrect.");
                    debug.write(Lvl.ERROR, this,
                            "File size should be: " + pred_size + "\n" +
                                    "But it's really: " + data_store_size);
                    need_repair = true;
                }

                // Move seek to sector start offset.
                data_store.seek(sector_offset);

                // Do we need to repair?
                if (need_repair) {
                    debug.write(Lvl.ALERT, this, "Store not closed cleanly.");
                }

                // Return true if we repaired.
                return need_repair;

            } else {
                throw new IOException(
                        "Sector size for this data store does not match.");
            }
        } else {
            throw new IOException("Format invalid; MAGIC number didn't match.");
        }

    }

    /**
     * Closes the data store.
     */
    public void close() throws IOException {
        // Sync internal information
        synch();
        // Write a '0' in the 'open' header section to indicate the store
        // closed cleanly.
        // ( Only if we opening in read/write mode )
        if (!read_only) {
            data_store.seek(28);
            data_store.writeByte(0);
        }
        // Check the size
        long close_size = data_store.length();
        if (close_size != data_store_size) {
            debug.write(Lvl.ERROR, this,
                    "On closing file, data_store_size != close_size (" +
                            data_store_size + " != " + close_size + ")");
        }

        // Sync the file with the hardware,
        try {
            data_store.getFD().sync();
        } catch (SyncFailedException e) { /* ignore */ }

        // Close the file
        data_store.close();
        // Help the GC
        data_store = null;
        sector_buffer = null;
        buffered_sector = -2;

    }

    /**
     * Returns true if the store is closed.
     */
    public boolean isClosed() {
        return data_store == null;
    }

    /**
     * Deletes the data store from the file system.
     */
    public void delete() {
        if (data_store == null) {
            data_file.delete();
        } else {
            throw new Error("Must close before FixedSizeDataStore is deleted.");
        }
    }


    /**
     * Returns true if the file for this store exists.
     */
    public boolean exists() throws IOException {
        return data_file.exists();
    }

    /**
     * Returns the number of bytes that the user may store in a sector.  The
     * actual sector space in the file may be slightly larger.
     */
    public int getSectorSize() {
        return sector_size - EXTRA_SECTOR_SIZE;
    }

    /**
     * Returns the number of sectors in the store that are being used (as
     * opposed to being deleted).
     */
    public int getSectorUseCount() {
        return used_sector_count;
    }

    /**
     * Returns the total number of sectors that are currently available
     * (includes used and deleted sectors).
     */
    public int rawSectorCount() throws IOException {
        return sectorCount();
    }


    // ------- Locking -------

    /**
     * Locks the store by some process so that we may not reclaim deleted
     * sectors.  The purpose of this is in the situation where we have a slow
     * thread accessing information from the store, and a seperate thread
     * is still able to modifying (delete and add) to the store.
     */
    public void lock() {
        ++lock_count;
    }

    /**
     * Unlocks the store.
     */
    public void unlock() {
        --lock_count;
        if (lock_count < 0) {
            throw new Error("Unlocked more times than we locked.");
        }
    }


    // ------- Sector queries --------

    /**
     * Returns true if the sector number is flagged as deleted.  If returns false
     * then the sector is being used.
     */
    public boolean isSectorDeleted(int sector) throws IOException {
        readSector(sector);
        return ((sector_buffer[0] & DELETED) != 0);
    }

    // ------- Get a sector from the store -------

    /**
     * Gets the contents of the sector at the given index.
     */
    public byte[] getSector(int sector, byte[] buf,
                            int offset, int length) throws IOException {
        if (sector >= sectorCount()) {
            throw new IOException("Can't get sector, out of range.");
        }

        int ssize = getSectorSize();
        if (length > ssize) {
            throw new IOException("length > sector size");
        }
        readSector(sector);
        System.arraycopy(sector_buffer, EXTRA_SECTOR_SIZE, buf, offset, length);
        return buf;
    }

    /**
     * Gets the contents of the sector at the given index.
     */
    public byte[] getSector(int sector, byte[] buf) throws IOException {
        return getSector(sector, buf, 0,
                Math.min(buf.length, getSectorSize()));
    }

    /**
     * Gets the contents of the sector at the given index.
     */
    public byte[] getSector(int sector) throws IOException {
        if (sector >= sectorCount()) {
            throw new IOException("Can't get sector, out of range.");
        }

        int ssize = getSectorSize();
        byte[] buf = new byte[ssize];
        readSector(sector);
        System.arraycopy(sector_buffer, EXTRA_SECTOR_SIZE, buf, 0, ssize);
        return buf;
    }

    /**
     * Gets the contents of the sector at the given index as an int[] array.
     * The array size is /4 of the sector size.  If the sector size is not
     * divisible by 4 then the last 1-3 bytes are truncated.
     */
    public int[] getSectorAsIntArray(int sector, int[] buf) throws IOException {
        if (sector >= sectorCount()) {
            throw new IOException("Can't get sector, out of range.");
        }

        int length = buf.length * 4;
        int ssize = getSectorSize();
        if (length > ssize) {
            throw new IOException("length > sector size");
        }
        readSector(sector);

        // Convert the sector (as a byte array) to an int array.
        int p = EXTRA_SECTOR_SIZE;
        int i = 0;
        while (i < buf.length) {

            int c1 = (((int) sector_buffer[p++]) & 0x0FF);
            int c2 = (((int) sector_buffer[p++]) & 0x0FF);
            int c3 = (((int) sector_buffer[p++]) & 0x0FF);
            int c4 = (((int) sector_buffer[p++]) & 0x0FF);
            int v = (c1 << 24) + (c2 << 16) + (c3 << 8) + (c4);

            buf[i++] = v;
        }

        return buf;

    }


    /**
     * Reads information across a chain of sectors and fills the byte[] array
     * buffer.  Returns the number of bytes that were read (should always be
     * equal to 'length').
     */
    public int readAcross(int sector_head, byte[] buf, int offset, int length)
            throws IOException {
        if (sector_head >= sectorCount()) {
            throw new IOException("Can't get sector, out of range.");
        }

        int to_read = length;
        int ssize = getSectorSize();

        int walk = sector_head;

        while (walk != -1 && to_read > 0) {

            // Read in the sector
            readSector(walk);
            // Is the sector deleted?
            if ((sector_buffer[0] & DELETED) != 0) {
                throw new IOException("Can not read across a deleted chain.");
            }
            // The next sector in the chain...
            int next_walk = ByteArrayUtil.getInt(sector_buffer, 1);

            // Fill the byte[] array buffer with what's in the sector.
            int amount_read = Math.min(to_read, ssize);
            System.arraycopy(sector_buffer, EXTRA_SECTOR_SIZE, buf, offset,
                    amount_read);

            offset += amount_read;
            to_read -= amount_read;

            // Walk to next in chain
            walk = next_walk;

        }

        return offset;

    }

    /**
     * Traverses a sector chain and returns an array of all sectors that are
     * part of the chain.
     * Useful for diagnostic, repair and statistical operations.
     */
    public int[] getSectorChain(int sector_head, int length) throws IOException {

        if (sector_head >= sectorCount()) {
            throw new IOException("Can't get sector, out of range.");
        }

        // The number of sectors to traverse.
        int span_count = calculateSectorSpan(length);

        int[] spans = new int[span_count];

        int ssize = getSectorSize();
        int walk = sector_head;
        int chain_count = 0;

        while (chain_count < span_count) {

            spans[chain_count] = walk;

            // Read in the sector
            readSector(walk);
            // The next sector in the chain...
            walk = ByteArrayUtil.getInt(sector_buffer, 1);

            // Increment the chain walk counter.
            ++chain_count;

        }

        return spans;

    }

    /**
     * Traverses a sector chain and returns an array of all sectors that are
     * part of the chain.
     * Useful for diagnostic, repair and statistical operations.
     */
    public int[] getSectorChain(int sector_head) throws IOException {

        if (sector_head >= sectorCount()) {
            throw new IOException("Can't get sector, out of range.");
        }

        IntegerVector spans = new IntegerVector();

        int ssize = getSectorSize();
        int walk = sector_head;

        while (walk > -1) {
            spans.addInt(walk);
            // Read in the sector
            readSector(walk);
            // The next sector in the chain...
            walk = ByteArrayUtil.getInt(sector_buffer, 1);
        }

        return spans.toIntArray();

    }

    // ------- Delete a sector from the store -------

    /**
     * Deletes a sector from the store.  The sector is only marked as deleted,
     * however, and the contents may still be accessed via the 'getSector'
     * methods.  If the store is add locked, then it is guarenteed that no
     * deleted sectors will be overwritten until the add lock is taken from the
     * table.
     * <p>
     * Throws an IO error if the sector is marked as deleted.
     */
    public void deleteSector(int sector) throws IOException {
        deleteAcross(sector);
    }

    /**
     * Deletes a set of sectors that have been chained together.  This should
     * be used to delete data added via the 'write' method.  However, it
     * can be used to delete data added via the 'addSector'
     */
    public void deleteAcross(final int sector_head) throws IOException {

        if (sector_head < 0) {
            throw new IOException("Sector is out of range.");
        }

        if (sector_head >= sectorCount()) {
            throw new IOException("Can't get sector, out of range.");
        }

        // How algorithm works:
        //   delete_head is set to sector_head
        //   We then walk through the chain until we hit a -1 and then we set
        //     that to the old delete_head.
        // NOTE: This algorithm doesn't change any chained sectors, so the
        //   'readAcross' method will still work even on deleted chains (provided
        //   there's a lock on the store).

        int walk = sector_head;

        while (walk != -1) {

            // Read in the sector
            readSector(walk);
            if ((sector_buffer[0] & DELETED) != 0) {
                // Already been deleted, so throw an IOException
                throw new IOException("Sector has already been deleted.");
            }

            // The next sector in the chain...
            int next_walk = ByteArrayUtil.getInt(sector_buffer, 1);

            // Mark as deleted
            sector_buffer[0] = DELETED;
            // If the next chain is -1 (end of chain) then set it to delete_head
            if (next_walk == -1) {
                ByteArrayUtil.setInt(delete_head, sector_buffer, 1);
            }
            // Write the new header for the sector.
            seekSector(walk);
            data_store.write(sector_buffer, 0, 5);
            if (sector_cache != null) {
                // Remove this from the cache.
                sector_cache.remove(new Integer(walk));
            }
            // Delete 1 from the used sector count.
            --used_sector_count;

            // Walk to next in chain
            walk = next_walk;

        }

        // Add this chain to the deleted chain.
        delete_head = sector_head;
        // Synchronize with the file system.
        synch();

    }

    /**
     * Deletes all sectors in the entire store.  Use with care.
     */
    public void deleteAllSectors() throws IOException {
        int sector_count = sectorCount();
        for (int i = 0; i < sector_count; ++i) {
            readSector(i);
            sector_buffer[0] = DELETED;
            int next = i + 1;
            if (i == sector_count - 1) {
                next = -1;
            }
            ByteArrayUtil.setInt(next, sector_buffer, 1);
            writeSector(i);
        }
        // Set the head of the delete chain
        delete_head = sector_count == 0 ? -1 : 0;
        // set 'used_sector_count'
        used_sector_count = 0;
        // Sync the information with the file
        synch();
    }

    // ------- Adds a new sector into the store -------

    /**
     * Writes the contents of a sector into the store overwritting any
     * other information that may be stored there.  This is used as a rough
     * data editting command.
     */
    public int overwriteSector(int sector, byte[] buf, int offset, int length)
            throws IOException {
        int ssize = getSectorSize();
        if (length > ssize) {
            throw new IOException("Sector too large to add to store.");
        }

        // Write the sector entry,
        return writeBufToSector(sector, -1, buf, offset, length);

    }

    /**
     * Writes the contents of a sector into the store overwritting any
     * other information that may be stored there.  This is used as a rough
     * data editting command.
     */
    public int overwriteSector(int sector, byte[] buf) throws IOException {
        return overwriteSector(sector, buf, 0, buf.length);
    }

    /**
     * Adds a new sector into the store.  It finds a suitable sector to store
     * the information and returns the sector number.  If lock_count > 0 then
     * we do not reclaim deleted sectors, otherwise we do.
     */
    public int addSector(byte[] buf, int offset, int length) throws IOException {
        int ssize = getSectorSize();
        if (length > ssize) {
            throw new IOException("Sector too large to add to store.");
        }
        // Find a suitable sector to add the data into.
        int sector = findFreeSector();

        // Write a new sector buffer entry,
        return writeBufToSector(sector, -1, buf, offset, length);

    }

    /**
     * Adds a new sector into the store.  It finds a suitable sector to store
     * the information and returns the sector number.  If lock_count > 0 then
     * we do not reclaim deleted sectors, otherwise we do.
     */
    public int addSector(byte[] buf) throws IOException {
        return addSector(buf, 0, buf.length);
    }

    /**
     * Calculates the number of sectors the given length of bytes will span.
     */
    public int calculateSectorSpan(int length) {
        int sector_size = getSectorSize();
        int span_count = length / sector_size;
        // Special case, if length is zero then still use at least 1 sector,
        if (length == 0 || (length % sector_size) != 0) {
            ++span_count;
        }
        return span_count;
    }

    /**
     * Writes a byte[] array of data across as many sectors as it takes to store
     * the data.  Returns the index to the first sector that contains the
     * start of the data.
     */
    public int writeAcross(byte[] buf, int offset, int length)
            throws IOException {

        int sector_size = getSectorSize();

        // How many sectors does this data span?
        int span_count = calculateSectorSpan(length);

        // Get free sectors to write this buffer information to.
        int[] free_sectors = findFreeSectors(span_count);
        // Sort the list so we are writing forward in the file.
        Arrays.sort(free_sectors, 0, span_count);

        // Write the information to the sectors.
        int to_write = length;
        int to_offset = 0;

        for (int i = 0; i < span_count; ++i) {
            int sector = free_sectors[i];
            int next_sector;
            if (i < span_count - 1) {
                next_sector = free_sectors[i + 1];
            } else {
                next_sector = -1;
            }

            // Write the sector part to the store.
            writeBufToSector(sector, next_sector, buf, to_offset,
                    Math.min(to_write, sector_size));

            to_write -= sector_size;
            to_offset += sector_size;
        }

        // Return the first free sector...
        return free_sectors[0];

    }


    /**
     * The last sector output stream that was created.
     */
    private SectorOutputStream sector_output_stream;

    /**
     * Returns an OutputStream implementation that is used to write a stream
     * of information into this data store.  As data is written into the stream,
     * the data is flushed into this store at the next available sector.  When
     * the stream is closed, the entire contents of the stream will be contained
     * within the store.  A call to 'getSectorOfLastOutputStream' can be used to
     * return an index that is used to reference this stream of information in
     * the store.
     * <p>
     * NOTE: While an output stream returned by this method is not closed,
     *  it is unsafe to use any methods in the FixedSizeDataStore object.
     */
    public OutputStream getSectorOutputStream() throws IOException {
        sector_output_stream = new SectorOutputStream();
        return sector_output_stream;
    }

    /**
     * Returns the first sector the OutputStream returned by
     * 'getSectorOutputStream' wrote to.  This is the start of the chain.
     */
    public int getSectorOfLastOutputStream() {
        return sector_output_stream.first_sector;
    }

    /**
     * Returns the number of bytes that were written out by the last closed
     * output stream returned by 'getSectorOutputStream'.
     */
    public int getLengthOfLastOutputStream() {
        return sector_output_stream.count;
    }

    /**
     * Wipes the SectorOutputStream from this object.  This should be closed
     * after the stream is closed.
     */
    public void wipeLastOutputStream() {
        sector_output_stream = null;
    }

    /**
     * Returns an InputStream implementation that is used to read a stream of
     * information from the store.  This input stream will iterate through the
     * sector chain given.
     * <p>
     * NOTE: Using this InputStream, an end of stream identifier is never
     *   produced.  When the last sector in the chain is reached, the input
     *   stream will first read padding whitespace, then it will either loop to
     *   the start of the last sector, or move to another undefined sector.
     *   You must not rely on this stream reaching an EOF.
     */
    public InputStream getSectorInputStream(int sector_head) throws IOException {
        return new SectorInputStream(sector_head);
    }

    // ------- Utility methods -------

    /**
     * Copies the entire contents of this store to a destination directory.
     * This can only be called when the data store is open.  It makes an
     * exact copy of the file.
     * <p>
     * The purpose of this method is so we can make a copy of the data
     * in this store while the store is open and 'live'.
     * <p>
     * We assume synchronization on this object.
     * <p>
     * @param path the directory to copy this file to.
     */
    public void copyTo(File path) throws IOException {
        String fname = data_file.getName();

        FileOutputStream fout = new FileOutputStream(new File(path, fname));
        int BUF_SIZE = 65536;     // 64k copy buffer.
        byte[] buf = new byte[BUF_SIZE];

        data_store.seek(0);
        int read = data_store.read(buf, 0, BUF_SIZE);
        while (read >= 0) {
            fout.write(buf, 0, read);
            read = data_store.read(buf, 0, BUF_SIZE);
        }

        fout.close();

    }

    /**
     * Attempts to repair this data store to a correct state.  The UserTerminal
     * object can be used to ask the user questions and to output information
     * on the progress of the repair.
     * <p>
     * The store must have been opened before this method is called.
     */
    public void fix(UserTerminal terminal) throws IOException {

        terminal.println("- File: " + data_file);

        // First synch with the disk
        synch();

        // Check the length is correct
        if ((data_store_size - (long) sector_offset) %
                (long) sector_size != 0) {
            terminal.println("+ Altering length of file so it is correct " +
                    "for sector size");
            int row_count = sectorCount() + 1;
            long new_size = (row_count * sector_size) + sector_offset;
            setDataStoreSize(new_size);
        }

        IntegerVector sector_info = new IntegerVector();
        IntegerVector scc = new IntegerVector();
        int null_count = 0;

        // The total number of physical sectors in the file,
        int sector_count = sectorCount();
        terminal.println("- Sector Count: " + sectorCount());

        // Go through every sector and mark each one appropriately.
        for (int i = 0; i < sector_count; ++i) {
            readSector(i);
            // Deleted sector
            int next_chain = ByteArrayUtil.getInt(sector_buffer, 1);
            sector_info.addInt((int) sector_buffer[0]);
            sector_info.addInt(next_chain);

            if (next_chain == -1) {
                ++null_count;
            } else {
                int old_val = 0;
                if (next_chain < scc.size()) {
                    old_val = scc.intAt(next_chain);
                }
                scc.placeIntAt(old_val + 1, next_chain);
            }
        }

        // The number of unchanged sectors...
        terminal.println("- unchained sectors = " + null_count);
        // Any sectors that are referenced more than once are erroneous.
        // These sectors are marked as bad
        IntegerVector bad_sectors = new IntegerVector();
        for (int i = 0; i < scc.size(); ++i) {
            int ref_count = scc.intAt(i);
            if (ref_count > 1) {
                terminal.println("- [" + i + "] reference count = " + ref_count);
                terminal.println("+ Marking all references as bad (except first).");
                boolean found_first = false;
                for (int n = 0; n < sector_info.size(); n += 2) {
                    if (sector_info.intAt(n + 1) == i) {
                        if (found_first) {
                            bad_sectors.addInt(n / 2);
                        }
                        found_first = true;
                    }
                }
            }
        }

        // Any marked as bad?
        if (bad_sectors.size() > 0) {
            terminal.println("+ Marked " + bad_sectors.size() + " sectors bad.");
        }

        // Mark the sectors as deleted
        for (int i = 0; i < bad_sectors.size(); ++i) {
            int sector = bad_sectors.intAt(i);
            readSector(sector);
            sector_buffer[0] = DELETED;
            writeSector(sector);
        }

        // PENDING: Are there are chains from active to deleted sectors, or
        // deleted to active.


        // Then go ahead and repair the file,
        repair();

    }

    /**
     * Cleans up so all deleted sectors are completely removed from the store.
     * This has the effect of reducing the size of the file by the size of every
     * deleted sector.
     * <p>
     * It is extremely important that nothing can be read/written from the file
     * while this is happening.  And certainly, we can not have any locks on
     * this store.
     * <p>
     * Returns true if the layout of the sectors changed (so we can fix
     * indices that point to sectors).
     */
    public boolean clearDeletedSectors() throws IOException {

        if (locked()) {
            throw new IOException(
                    "Store is locked, can not reclaim deleted sectors.");
        }

        // Are there any deleted rows to reclaim?
        if (delete_head != -1) {

            // Yes, so run through the table and move all data over the top of
            // deleted rows.
            int scount = sectorCount();
            int move_to = 0;
            int row_count = 0;
            for (int i = 0; i < scount; ++i) {
                // Read the sector
                readSector(i);
                // Is it used?  (DELETED flag not set)
                if ((sector_buffer[0] & DELETED) == 0) {
                    ++row_count;
                    // Not deleted, therefore we may have to move.  Is move_to < i?
                    if (move_to < i) {
                        // Move this sector to 'move_to'
                        writeSector(move_to);
                        buffered_sector = move_to;
                    }
                    move_to = move_to + 1;
                }
            }

            // Resize the file.
            long new_size = (row_count * sector_size) + sector_offset;
            setDataStoreSize(new_size);
            // Set the delete_head to -1
            delete_head = -1;
            // The number of sectors that are being used.
            used_sector_count = row_count;
            // Synchronize the header.
            synch();
            // Sectors moved around so return true.
            return true;

        } else {
            // No rows to remove so return false.
            return false;
        }
    }

// [ It's a bad idea to use this when there are sector chains because it
//   reorganizes the chain of deleted sectors.  The order of deleted sectors is
//   important when dirty reading deleted information from the store (when a
//   table is updated for example).
//   In addition, it's debatable whether a 'repair' method is worth it.  It
//   would probably be better to use 'clearDeletedSectors' to ensure the
//   store is in a good state. ]

    /**
     * Repairs the consistancy of the store.  This is an expensive operation
     * that runs through every sector and determines if it's deleted or used.
     * If it's deleted it is added into the deleted linked list.
     * <p>
     * Repair assumes we can at least get past the 'open' method.  This method
     * does not change the order of the sectors in the store.  However it may
     * change the order in which deleted sectors are reclaimed.
     * <p>
     * In a perfect world, this should never need to be called.  However, it's
     * a good idea to call this every so often because we are assured that
     * the delete linked list and 'used_sector_count' variables will be
     * correct when the method returns.
     * <p>
     * It is not possible to repair a store that's been opened in read only
     * mode.
     */
    public void repair() throws IOException {

        // Init to known states.
        delete_head = -1;
        int scount = sectorCount();
        int row_count = 0;
        int delete_count = 0;

        byte[] mark_buffer = new byte[5];

        for (int i = 0; i < scount; ++i) {
            // Read the sector
            readSector(i);
            // Is it deleted?
            if ((sector_buffer[0] & DELETED) != 0) {
                // Add this row into the list of deleted rows.
                int v = delete_head;
                mark_buffer[0] = DELETED;
                mark_buffer[1] = (byte) ((v >>> 24) & 0xFF);
                mark_buffer[2] = (byte) ((v >>> 16) & 0xFF);
                mark_buffer[3] = (byte) ((v >>> 8) & 0xFF);
                mark_buffer[4] = (byte) ((v >>> 0) & 0xFF);
                seekSector(i);
                data_store.write(mark_buffer, 0, 5);
                if (sector_cache != null) {
                    // Remove from cache
                    sector_cache.remove(new Integer(i));
                }
                delete_head = i;

                ++delete_count;
            } else {
                // Add to the used sector count
                ++row_count;
            }
        }

        // 'delete_head' should be set correctly now,
        // set 'used_sector_count'
        used_sector_count = row_count;
        // Sync the information with the file
        synch();

        debug.write(Lvl.MESSAGE, this,
                "Repair found (" + delete_count + ") deleted, (" +
                        row_count + ") used sectors.");
    }

    // ------- Diagnostics for the store -------

    /**
     * Returns a string that contains diagnostic information.
     */
    public String statusString() throws IOException {
        int sc = sectorCount();

        StringBuffer str = new StringBuffer();
        str.append("Sector Count: ");
        str.append(sc);
        str.append("\nSectors Used: ");
        str.append(getSectorUseCount());
        str.append("\nLocks: ");
        str.append(lock_count);
        str.append("\nFree Sectors: ");
        str.append(sc - getSectorUseCount());
        str.append("\n");

        return new String(str);
    }

    // ---------- Inner classes ----------

    /**
     * A buffered OutputStream object that writes all data written to the stream
     * out to the store.
     */
    private class SectorOutputStream extends OutputStream {

        /**
         * The sector buffers.
         */
        private final byte[] buf;

        /**
         * The first sector we wrote to.
         */
        private int first_sector = -1;

        /**
         * The cur sector to use.
         */
        private int cur_sector = -1;

        /**
         * The last sector we wrote to.
         */
        private int last_sector = -1;

        /**
         * Current index in the buffer
         */
        private int index;

        /**
         * Total bytes written.
         */
        private int count;

        SectorOutputStream() throws IOException {
            buf = new byte[getSectorSize()];
            index = 0;
            count = 0;
            first_sector = findFreeSector();
            cur_sector = first_sector;
        }

        // ---------- Implemented from OutputStream ----------

        public void write(int b) throws IOException {
            if (index >= buf.length) {
                // Flush to the next sector.

                int next_sector = findFreeSector();
                if (next_sector == cur_sector) {
                    // Nasty hack - if next_sector == cur_sector then we reclaiming
                    //  space from end of store, so increment by 1.
                    next_sector = next_sector + 1;
                }

                // Write the buffer.
                writeBufToSector(cur_sector, next_sector, buf, 0, index);
                cur_sector = next_sector;
                index = 0;
            }

            buf[index] = (byte) b;
            ++index;
            ++count;

        }

        public void write(byte[] b, int offset, int len) throws IOException {
            while (index + len > buf.length) {
                // Copy
                int to_copy = buf.length - index;
                System.arraycopy(b, offset, buf, index, to_copy);
                offset += to_copy;
                len -= to_copy;
                index += to_copy;   // Not really necessary - just gets set to 0
                count += to_copy;

                int next_sector = findFreeSector();
                if (next_sector == cur_sector) {
                    // Nasty hack - if next_sector == cur_sector then we reclaiming
                    //  space from end of store, so increment by 1.
                    next_sector = next_sector + 1;
                }
                writeBufToSector(cur_sector, next_sector, buf, 0, index);
                cur_sector = next_sector;

                index = 0;
            }

            if (len > 0) {
                System.arraycopy(b, offset, buf, index, len);
                index += len;
                count += len;
            }

        }

        public void flush() throws IOException {
            // Flush does nothing...
        }

        public void close() throws IOException {
            writeBufToSector(cur_sector, -1, buf, 0, index);
        }

    }

    /**
     * An input stream that reads information across a sector chain starting at
     * the given head sector.
     */
    private final class SectorInputStream extends InputStream {

        /**
         * The current sector we are traversing.
         */
        private int sector;

        /**
         * Current index in buf.
         */
        private int index;

        /**
         * The number of bytes we have read.
         */
        private int count;

        /**
         * A reference to the sector buffer.
         */
        private final byte[] sector_buffer;


        /**
         * Constructor.
         */
        SectorInputStream(int sector_head) throws IOException {
            this.sector = sector_head;
            this.sector_buffer = FixedSizeDataStore.this.sector_buffer;

            // Load the first sector.
            loadNextSector();

            count = 0;
        }

        /**
         * Loads the next sector in the chain into sector_buffer and sets index
         * to the start of the buffer.
         */
        private void loadNextSector() throws IOException {
            if (sector != -1) {
                // Read contents into 'sector_buffer'
                readSector(sector);
            }
            index = EXTRA_SECTOR_SIZE;
            // The next sector
            sector = ByteArrayUtil.getInt(sector_buffer, 1);
        }

        // ---------- Implemented from InputStream ----------

        public final int read() throws IOException {

            int b = ((int) sector_buffer[index]) & 0x0FF;
            ++index;
            ++count;
            if (index >= sector_size) {
                loadNextSector();
            }
            return b;

        }

        public int read(byte[] b, int offset, int len) throws IOException {
            int original_len = len;

            while (index + len > sector_size) {
                // Copy
                int to_copy = sector_size - index;
                System.arraycopy(sector_buffer, index, b, offset, to_copy);
                offset += to_copy;
                len -= to_copy;
                index += to_copy;   // Not really necessary - just gets set to 0
                count += to_copy;

                // Load the next sector.
                loadNextSector();

            }

            if (len > 0) {
                System.arraycopy(sector_buffer, index, b, offset, len);
                index += len;
                count += len;

                if (index >= sector_size) {
                    loadNextSector();
                }

            }

            return original_len;
        }

        public long skip(long len) throws IOException {
            long original_len = len;

            while (index + len > sector_size) {
                int to_copy = sector_size - index;
                len -= to_copy;
                index += to_copy;   // Not really necessary - just gets set to 0
                count += to_copy;

                // Load the next sector.
                loadNextSector();

            }

            if (len > 0) {
                index += len;
                count += len;

                if (index >= sector_size) {
                    loadNextSector();
                }

            }

            return original_len;
        }

    }

}
