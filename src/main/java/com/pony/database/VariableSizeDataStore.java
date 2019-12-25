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

import com.pony.debug.*;
import com.pony.util.ByteArrayUtil;
import com.pony.util.UserTerminal;

import java.io.*;
import java.util.zip.*;

/**
 * Provides a mechanism for storing variable length data in a file which can
 * quickly be indexed via a reference number.  The store maintains a master
 * index file that contains a reference to all individual data records stored
 * in the system.  The data file contains all the data that has been stored.
 * <p>
 * This file format is not intended to be a fully fledged file system.  For
 * example, we can not easily change the size of a data entry.  To change the
 * record, we must delete it then add a new.
 * <p>
 * This system uses two files.  One file is the index, and the second file
 * stores the data.  The index file contains sectors as follows:
 * <p><pre>
 *   4 (int) : index  - The sector index of the data in the data file.
 *   4 (int) : length - Length of the data that was stored.
 *   4 (int) : status - 32 status bits.
 * </pre>
 * <p>
 * This employs a simple compression scheme when writing out data that would
 * span more than one sector.  It tries compressing the field.  If the field
 * can be compressed into less sectors than if left uncompressed, then the
 * compressed field is put into the data store.
 *
 * @author Tobias Downer
 */

public class VariableSizeDataStore {

    /**
     * Set to true to enable compressed writes.
     */
    private final static boolean COMPRESSED_WRITE_ENABLED = true;

    /**
     * The size of elements of each allocation sector.
     */
    private final static int INDEX_SECTOR_SIZE = (4 * 3);


    /**
     * A DebugLogger object used to log error messages to.
     */
    private final DebugLogger debug;

    /**
     * The index data allocation file.
     */
    private final FixedSizeDataStore allocation_store;

    /**
     * The actual data file.
     */
    private final FixedSizeDataStore data_store;

    /**
     * A buffer to store the index key.
     */
    private final byte[] index_key;

    /**
     * A Deflater and Inflater used to compress and uncompress the size of data
     * fields put into the store.
     */
    private Deflater deflater;
    private Inflater inflater;
    private byte[] compress_buffer;


    /**
     * Constructs the variable size store.
     */
    public VariableSizeDataStore(File name, int sector_size,
                                 DebugLogger logger) {
        this.debug = logger;
        index_key = new byte[INDEX_SECTOR_SIZE];

        // We create two files, name + ".axi" and name + ".dss"
        // The .axi file is the allocation index.  The .dss is the data sector
        // storage.
        String path = name.getPath();
        allocation_store = new FixedSizeDataStore(new File(path + ".axi"),
                INDEX_SECTOR_SIZE, debug);
        data_store = new FixedSizeDataStore(new File(path + ".dss"),
                sector_size, debug);
    }

    public VariableSizeDataStore(File name, DebugLogger logger) {
        this(name, -1, logger);
    }


    // ---------- Private methods ----------


    // ---------- Public methods ----------

    /**
     * Synchronizes all the data in memory with the hard copy on disk.
     */
    public void synch() throws IOException {
        allocation_store.synch();
        data_store.synch();
    }

    /**
     * Hard synchronizes the data from memory into the hard copy on disk.  This
     * is guarenteed to ensure the image on the disk will match the image in
     * memory.
     */
    public void hardSynch() throws IOException {
        allocation_store.hardSynch();
        data_store.hardSynch();
    }

    /**
     * Returns true if we are locked.
     */
    public boolean locked() {
        return allocation_store.locked();
    }

    /**
     * Locks the store so that not deleted elements may be overwritten.
     */
    public void lock() {
        allocation_store.lock();
        data_store.lock();
    }

    /**
     * Unlocks the store so that deleted elements can be reclaimed again.
     */
    public void unlock() {
        data_store.unlock();
        allocation_store.unlock();
    }


    /**
     * Returns true if the given record is marked as deleted or not.
     */
    public boolean recordDeleted(int record_index) throws IOException {
        return allocation_store.isSectorDeleted(record_index);
    }

    /**
     * Returns the number of records that are being used.
     */
    public int usedRecordCount() {
        return allocation_store.getSectorUseCount();
    }

    /**
     * Returns the total number of records that are in the store (including
     * deleted records.
     */
    public int rawRecordCount() throws IOException {
        return allocation_store.rawSectorCount();
    }

    /**
     * Returns true if the data store exists.
     */
    public boolean exists() throws IOException {
        return allocation_store.exists() && data_store.exists();
    }

    /**
     * Returns true if the store was openned in read only mode.
     */
    public boolean isReadOnly() {
        return data_store.isReadOnly();
    }

    /**
     * Opens the data store.  The data store can be opened in 'read only' mode.
     * Returns 'true' if the open procedure should repair itself (dirty open)
     * or false if the file was cleanly closed down.
     * <p>
     * It is not possible to open a damaged store in read only mode.
     *
     * @param read_only if true, then the database is opened in read only mode,
     *   otherwise it is opened in read/write mode.
     */
    public boolean open(boolean read_only) throws IOException {
        boolean r1 = allocation_store.open(read_only);
        boolean r2 = data_store.open(read_only);
        return r1 | r2;
    }

    /**
     * Closes the data store.
     */
    public void close() throws IOException {
        allocation_store.close();
        data_store.close();
    }

    /**
     * Deletes the store from the file system.  Must be called after a 'close'.
     */
    public void delete() {
        allocation_store.delete();
        data_store.delete();
    }

    /**
     * Attempts to fix a corrupt VariableSizeDataStore object.
     * <p>
     * The store should be open before this method is called.
     */
    public void fix(UserTerminal terminal) throws IOException {
        terminal.println("+ Fixing variable data store.");
        // First fix the allocation store and data store.
        allocation_store.fix(terminal);
        data_store.fix(terminal);

        terminal.println("- Repairing references.");
        // Now look for bad references on this layer.  What we do here is we check
        // that each allocation record references a valid chain in the data
        // store.  If it doesn't then we delete it.
        int sector_count = allocation_store.rawSectorCount();
        int data_sector_count = data_store.rawSectorCount();
        terminal.println("- Sector count: " + sector_count);
        // For each allocation entry trace its chain and mark each sector as
        // taken.
        int bad_record_count = 0;
        int deleted_record_count = 0;
        for (int i = 0; i < sector_count; ++i) {
            if (!allocation_store.isSectorDeleted(i)) {
                allocation_store.getSector(i, index_key);
                int sector_head = ByteArrayUtil.getInt(index_key, 0);
                int length = ByteArrayUtil.getInt(index_key, 4);
                // Is the sector head pointing to a valid record?
                if (sector_head < 0 || sector_head >= data_sector_count ||
                        length <= 0) {
                    ++bad_record_count;
                    // Mark this allocation entry as deleted.
                    allocation_store.deleteAcross(i);
                    ++deleted_record_count;
                } else {
                    int[] chain_span = data_store.getSectorChain(sector_head, length);
                }
            } else {
                ++deleted_record_count;
            }
        }
        // Print statistics,
        terminal.println("- Fixed " + bad_record_count + " bad chains.");

    }

    /**
     * Copies this data store to the given path.  The store must be open when
     * this is called.
     */
    public void copyTo(File path) throws IOException {
        allocation_store.copyTo(path);
        data_store.copyTo(path);
    }

    /**
     * Updates the 32-bit type_key int of a record.  Bit 1-8 are reserved for
     * this data store, and are used to indicate such things as whether the
     * record chain is compressed or not.  The rest of the bits can be used
     * for any purpose.  It is recommended bits 8 through 16 are used for
     * user-definable information.
     */
    public int writeRecordType(int record_index, int type_key)
            throws IOException {
        // Read it in first,
        // The index of the record to retrieve.
        allocation_store.getSector(record_index, index_key);
        // Any special keys regarding how the info was stored
        int cur_type_key = ByteArrayUtil.getInt(index_key, 8);
        // Record this.
        final int old_type_key = cur_type_key;
        // Merge type key
        type_key = (type_key & 0x0FFFFFFF0) | (cur_type_key & 0x0F);
        ByteArrayUtil.setInt(type_key, index_key, 8);
        // And overwrite the sector
        allocation_store.overwriteSector(record_index, index_key);

        // Return the type key as it was before the change.
        return old_type_key;
    }

    /**
     * Reads the 32-bit type_key int for the given record.  The 'type_key'
     * contains various bit flags set for the record.
     */
    public int readRecordType(int record_index) throws IOException {
        // Read it in first,
        // The index of the record to retrieve.
        allocation_store.getSector(record_index, index_key);
        // Any special keys regarding how the info was stored
        int cur_type_key = ByteArrayUtil.getInt(index_key, 8);

        // Return the type key for this record.
        return cur_type_key;
    }

    /**
     * Writes a variable length byte[] array to the first available index.
     * Returns the index reference for this element.
     */
    public int write(byte[] buf, int offset, int length) throws IOException {

        // If the length of the record to add is bigger than a sector then try
        // and compress it.
        int sector_size = data_store.getSectorSize();
        boolean use_compressed_form = false;

        int compress_size = -1;
        if (COMPRESSED_WRITE_ENABLED) {
            if (length > sector_size) {
                int orig_span = data_store.calculateSectorSpan(length);

                if (deflater == null) {
                    deflater = new Deflater();
                }
                deflater.setInput(buf, offset, length);
                deflater.finish();

                if (compress_buffer == null || compress_buffer.length < length + 4) {
                    compress_buffer = new byte[length + 4];
                }
                compress_size = deflater.deflate(compress_buffer) + 4;
                deflater.reset();

                int new_span = data_store.calculateSectorSpan(compress_size);
                if (new_span < orig_span) {
                    // Put the length of the original buffer on the end of the compressed
                    // data.
                    ByteArrayUtil.setInt(length, compress_buffer, compress_size - 4);
                    use_compressed_form = true;
                }

            }
        }

        // Write the data to the data file,
        int v;
        int real_length;
        int type_key = 0;
        if (use_compressed_form) {
            v = data_store.writeAcross(compress_buffer, 0, compress_size);
            real_length = compress_size;
            // Indicate this run is compressed.
            type_key = type_key | 0x0001;
        } else {
            v = data_store.writeAcross(buf, offset, length);
            real_length = length;
        }
        // Create a new index key,
        // The first index
        ByteArrayUtil.setInt(v, index_key, 0);
        ByteArrayUtil.setInt(real_length, index_key, 4);
        ByteArrayUtil.setInt(type_key, index_key, 8);

        // Add to the allocation store last.
        return allocation_store.addSector(index_key);
    }

    /**
     * Reads a variable length byte[] array from the given index position.
     * This will read the first n bytes from the element, upto the maximum that
     * was stored.  It returns the number of bytes that were read.
     */
    public int read(int record, byte[] buf, int offset, int length)
            throws IOException {

        // The index of the record to retrieve.
        allocation_store.getSector(record, index_key);
        // Get the head of the chain,
        int chain_head = ByteArrayUtil.getInt(index_key, 0);
        // The length of data that was stored.
        int data_length = ByteArrayUtil.getInt(index_key, 4);
        // Any special keys regarding how the info was stored
        int type_key = ByteArrayUtil.getInt(index_key, 8);

        // If it's compressed, read in the compressed data to the buffer.
        if ((type_key & 0x0001) != 0) {
            if (compress_buffer == null || compress_buffer.length < data_length) {
                compress_buffer = new byte[data_length];
            }
            data_store.readAcross(chain_head, compress_buffer, 0, data_length);

            // Then extract as much as we can into the input buffer.
            if (inflater == null) {
                inflater = new Inflater();
            }
            inflater.reset();
            inflater.setInput(compress_buffer, 0, data_length);
            int inflate_count;
            try {
                inflate_count = inflater.inflate(buf, offset, length);
            } catch (DataFormatException e) {
                e.printStackTrace();
                debug.writeException(e);
                throw new Error(e.getMessage());
            }

            return inflate_count;
        } else {
            // Not compressed...
            // The amount we are reading,
            int read_amount = Math.min(length, data_length);
            // Read it in,
            data_store.readAcross(chain_head, buf, offset, read_amount);

            return read_amount;
        }

    }

    /**
     * Reads in a complete record and puts it into the returned byte[] array.
     */
    public byte[] readRecord(int record) throws IOException {
        // The index of the record to retrieve.
        allocation_store.getSector(record, index_key);
        // Get the head of the chain,
        int chain_head = ByteArrayUtil.getInt(index_key, 0);
        // The length of data that was stored.
        int data_length = ByteArrayUtil.getInt(index_key, 4);
        // Any special keys regarding how the info was stored
        int type_key = ByteArrayUtil.getInt(index_key, 8);

        // If it's compressed, read in the compressed data to the buffer.
        if ((type_key & 0x0001) != 0) {
            if (compress_buffer == null || compress_buffer.length < data_length) {
                compress_buffer = new byte[data_length];
            }
            data_store.readAcross(chain_head, compress_buffer, 0, data_length);

            // Then extract as much as we can into the input buffer.
            if (inflater == null) {
                inflater = new Inflater();
            }
            // Get the size of the uncompressed form...
            int uncompressed_size =
                    ByteArrayUtil.getInt(compress_buffer, data_length - 4);

            byte[] buf = new byte[uncompressed_size];

            inflater.reset();
            inflater.setInput(compress_buffer, 0, data_length - 4);
            int inflate_count;
            try {
                inflate_count = inflater.inflate(buf);
            } catch (DataFormatException e) {
                e.printStackTrace();
                debug.writeException(e);
                throw new Error(e.getMessage());
            }

            if (inflate_count != buf.length) {
                throw new Error("Inflate size != buf.length (" +
                        inflate_count + " != " + buf.length + ")");
            }

            return buf;
        } else {
            // Not compressed...
            // Allocate the buffer store.
            byte[] buf = new byte[data_length];
            // Read it in,
            data_store.readAcross(chain_head, buf, 0, data_length);

            return buf;
        }

    }

    /**
     * Deletes the data at the given index position.
     */
    public int delete(int record) throws IOException {
        // The index of the record to delete,
        allocation_store.getSector(record, index_key);
        // Get the head of the chain to delete.
        int chain_head = ByteArrayUtil.getInt(index_key, 0);

        // Delete the allocation index,
        allocation_store.deleteSector(record);
        // Delete the data chain,
        data_store.deleteAcross(chain_head);

        return record;
    }

    private OutputStream sector_output_stream = null;

    /**
     * Returns an OutputStream object that can be used to write data into the
     * store.  When the 'completeWriteStream' method is called, the records in
     * this store are updated appropriately for the data written in, and a
     * record index is returned.
     * <p>
     * NOTE: Only one open stream may be active at a time.  While this stream
     *   is open this VariableSizeDataStore object may not be used in any other
     *   way.
     */
    public OutputStream getRecordOutputStream() throws IOException {
        if (sector_output_stream == null) {
            sector_output_stream = data_store.getSectorOutputStream();
            return sector_output_stream;
        } else {
            throw new Error("More than one record output stream opened.");
        }
    }

    /**
     * Updates the record allocation table with the data in the output stream
     * returned by 'getRecordOutputStream'.  Returns an index for how to
     * reference this data later.
     * <p>
     * After this method is called it is safe again to use this
     * VariableSizeDataStore object.
     */
    public int completeRecordStreamWrite() throws IOException {
        if (sector_output_stream != null) {
            int v = data_store.getSectorOfLastOutputStream();
            int real_length = data_store.getLengthOfLastOutputStream();
            int type_key = 0;
            // Create a new index key,
            // The first index
            ByteArrayUtil.setInt(v, index_key, 0);
            ByteArrayUtil.setInt(real_length, index_key, 4);
            ByteArrayUtil.setInt(type_key, index_key, 8);

            sector_output_stream = null;
            data_store.wipeLastOutputStream();

            // Add to the allocation store last.
            return allocation_store.addSector(index_key);
        } else {
            throw new Error("Output stream not available.");
        }
    }

    /**
     * Returns an InputStream that is used to read a record in this store with
     * the given index.
     * <p>
     * NOTE: This can not handle compressed records.
     * <p>
     * NOTE: This does not detect the end of stream (reading past the end of the
     *   record will return undefined data).
     */
    public InputStream getRecordInputStream(int record) throws IOException {
        // The index of the record to read,
        allocation_store.getSector(record, index_key);
        // Get the head of the chain to read.
        int chain_head = ByteArrayUtil.getInt(index_key, 0);

        // Open the input stream.
        return data_store.getSectorInputStream(chain_head);
    }


    /**
     * Returns the size (in bytes) of the sectors used to store information in
     * the data file.
     */
    public int sectorSize() throws IOException {
        return data_store.getSectorSize();
    }

    /**
     * Returns the size of the given record number (compressed size if
     * applicable).
     */
    public int recordSize(int record) throws IOException {
        // The index of the record to retrieve.
        allocation_store.getSector(record, index_key);
        // Return the size of the record
        return ByteArrayUtil.getInt(index_key, 4);
    }

    /**
     * Returns true if the given record is compressed.
     */
    public boolean isCompressed(int record) throws IOException {
        // The index of the record.
        allocation_store.getSector(record, index_key);
        // Return true if the compressed bit is set.
        return (ByteArrayUtil.getInt(index_key, 8) & 0x0001) != 0;
    }

    /**
     * Returns the number of sectors the given record takes up in the data
     * store.
     */
    public int recordSectorCount(int record) throws IOException {
        // Returns the number of sectors a record of this size will span using
        // the current sector size.
        return data_store.calculateSectorSpan(recordSize(record));
    }

    /**
     * Returns the size of the data file that keeps all the data in this
     * store.  This is the file size of the data store.
     */
    public long totalStoreSize() {
        return data_store.totalSize();
    }

    /**
     * Writes reserved information to the variable data store.  You may only
     * write upto 128 bytes to the reserved data buffer.
     */
    public void writeReservedBuffer(byte[] info, int offset, int length,
                                    int res_offset) throws IOException {
        allocation_store.writeReservedBuffer(info, offset, length, res_offset);
    }

    public void writeReservedBuffer(byte[] info, int offset, int length)
            throws IOException {
        allocation_store.writeReservedBuffer(info, offset, length);
    }

    /**
     * Reads reserved information from the variable data store.  You may only
     * read upto 128 bytes from the reserved data buffer.
     */
    public void readReservedBuffer(byte[] info, int offset, int length)
            throws IOException {
        allocation_store.readReservedBuffer(info, offset, length);
    }


    // ---------- Static methods ----------

    /**
     * Convenience for checking if a given data store exists or not.  Returns
     * true if it exists.
     */
    public static boolean exists(File path, String name) throws IOException {
        File af = new File(path, name + ".axi");
        File df = new File(path, name + ".dss");

        return (af.exists() & df.exists());
    }

    /**
     * Convenience for deleting a VariableSizeDataStore store.
     */
    public static boolean delete(File path, String name) throws IOException {
        File af = new File(path, name + ".axi");
        File df = new File(path, name + ".dss");

        return (af.delete() & df.delete());
    }

    /**
     * Convenience for renaming a VariableSizeDataStore store to another name.
     */
    public static boolean rename(File path_source, String name_source,
                                 File path_dest, String name_dest) throws IOException {
        File afs = new File(path_source, name_source + ".axi");
        File dfs = new File(path_source, name_source + ".dss");
        File afd = new File(path_dest, name_dest + ".axi");
        File dfd = new File(path_dest, name_dest + ".dss");

        return (afs.renameTo(afd) & dfs.renameTo(dfd));
    }


    // ---------- Testing methods ----------

    public int writeString(String str) throws IOException {
        byte[] bts = str.getBytes();
        return write(bts, 0, bts.length);
    }

    public String readString(int record) throws IOException {
        byte[] buffer = new byte[65536];
        int read_in = read(record, buffer, 0, 65536);
        return new String(buffer, 0, read_in);
    }

}
