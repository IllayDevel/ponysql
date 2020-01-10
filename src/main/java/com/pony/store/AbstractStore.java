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

package com.pony.store;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.*;

import com.pony.util.ByteArrayUtil;
import com.pony.util.UserTerminal;

/**
 * Provides an abstract implementation of Store.  This implements a bin based
 * best-fit recycling algorithm.  The store manages a structure that points to
 * bins of freed space of specific sizes.  When an allocation is requested the
 * structure is searched for the first bin that contains an area that best fits
 * the size requested.
 * <p>
 * Provided the derived class supports safe atomic IO operations, this store
 * is designed for robustness to the level that at no point is the store left
 * in a unworkable (corrupt) state.
 *
 * @author Tobias Downer
 */

public abstract class AbstractStore implements Store {

    /**
     * The free bin list contains 128 entries pointing to the first available
     * block in the bin.  If the list item contains -1 then there are no free
     * blocks in the bin.
     */
    protected long[] free_bin_list;


    /**
     * A pointer to the wilderness area (the last deleted area in the store),
     * or -1 if there is no wilderness area.
     */
    protected long wilderness_pointer;

    /**
     * True if this is read-only.
     */
    protected final boolean read_only;

    /**
     * The total amount of allocated space within this store since the store
     * was openned.  Note that this could be a negative amount if more space
     * was freed than allocated.
     */
    protected long total_allocated_space;

    /**
     * True if the store was opened dirtily (was not previously closed cleanly).
     */
    private boolean dirty_open;

    // ---------- Statics ----------

    /**
     * The offset into the file that the data areas start.
     */
    protected static final long DATA_AREA_OFFSET = 256 + 1024 + 32;

    /**
     * The offset into the file of the 64 byte fixed area.
     */
    protected static final long FIXED_AREA_OFFSET = 128;

    /**
     * The offset into the file that the bin area starts.
     */
    protected static final long BIN_AREA_OFFSET = 256;

    /**
     * The magic value.
     */
    protected static final int MAGIC = 0x0A7A7AE;


    /**
     * Constructs the store.
     */
    protected AbstractStore(boolean read_only) {
        free_bin_list = new long[BIN_ENTRIES + 1];
        for (int i = 0; i < BIN_ENTRIES + 1; ++i) {
            free_bin_list[i] = -1;
        }
        wilderness_pointer = -1;
        this.read_only = read_only;
    }

    /**
     * Initializes the store to an empty state.
     */
    private synchronized void initializeToEmpty() throws IOException {
        setDataAreaSize(DATA_AREA_OFFSET);
        // New file so write out the initial file area,
        ByteArrayOutputStream bout =
                new ByteArrayOutputStream((int) BIN_AREA_OFFSET);
        DataOutputStream out = new DataOutputStream(bout);
        // The file MAGIC
        out.writeInt(MAGIC);   // 0
        // The file version
        out.writeInt(1);       // 4
        // The number of areas (chunks) in the file (currently unused)
        out.writeLong(-1);     // 8
        // File open/close status byte
        out.writeByte(0);      // 16

        out.flush();
        byte[] buf = new byte[(int) DATA_AREA_OFFSET];
        byte[] buf2 = bout.toByteArray();
        System.arraycopy(buf2, 0, buf, 0, buf2.length);
        for (int i = (int) BIN_AREA_OFFSET; i < (int) DATA_AREA_OFFSET; ++i) {
            buf[i] = (byte) 255;
        }

        writeByteArrayToPT(0, buf, 0, buf.length);
    }


    /**
     * Opens the data store.  Returns true if the store did not close cleanly.
     */
    public synchronized boolean open() throws IOException {
        internalOpen(read_only);

        // If it's small, initialize to empty
        if (endOfDataAreaPointer() < DATA_AREA_OFFSET) {
            initializeToEmpty();
        }

        byte[] read_buf = new byte[(int) BIN_AREA_OFFSET];
        readByteArrayFrom(0, read_buf, 0, read_buf.length);
        ByteArrayInputStream b_in = new ByteArrayInputStream(read_buf);
        DataInputStream din = new DataInputStream(b_in);

        int magic = din.readInt();
        if (magic != MAGIC) {
            throw new IOException("Format invalid: Magic value is not as expected.");
        }
        int version = din.readInt();
        if (version != 1) {
            throw new IOException("Format invalid: unrecognised version.");
        }
        din.readLong();  // ignore
        byte status = din.readByte();
        // This means the store wasn't closed cleanly.
        dirty_open = status == 1;

        // Read the bins
        readBins();

        // Mark the file as open
        if (!read_only) {
            writeByteToPT(16, 1);
        }

        long file_length = endOfDataAreaPointer();
        if (file_length <= 8) {
            throw new IOException("Format invalid: File size is too small.");
        }

        // Set the wilderness pointer.
        if (file_length == DATA_AREA_OFFSET) {
            wilderness_pointer = -1;
        } else {
            readByteArrayFrom(file_length - 8, read_buf, 0, 8);
            long last_boundary = ByteArrayUtil.getLong(read_buf, 0);
            long last_area_pointer = file_length - last_boundary;

            if (last_area_pointer < DATA_AREA_OFFSET) {
                System.out.println("last_boundary = " + last_boundary);
                System.out.println("last_area_pointer = " + last_area_pointer);
                throw new IOException(
                        "File corrupt: last_area_pointer is before data part of file.");
            }
            if (last_area_pointer > file_length - 8) {
                throw new IOException(
                        "File corrupt: last_area_pointer at the end of the file.");
            }

            readByteArrayFrom(last_area_pointer, read_buf, 0, 8);
            long last_area_header = ByteArrayUtil.getLong(read_buf, 0);
            // If this is a freed block, then set this are the wilderness pointer.
            if ((last_area_header & 0x08000000000000000L) != 0) {
                wilderness_pointer = last_area_pointer;
            } else {
                wilderness_pointer = -1;
            }
        }

        return dirty_open;
    }

    /**
     * Closes the store.
     */
    public synchronized void close() throws IOException {

        // Mark the file as closed
        if (!read_only) {
            writeByteToPT(16, 0);
        }

        internalClose();
    }

    /**
     * Returns true if the given area size is valid.  Currently the criteria
     * for a valid boundary size is (size >= 24) and (size % 8 == 0) and
     * (size < 200 gigabytes)
     */
    protected static boolean isValidBoundarySize(long size) {
        long MAX_AREA_SIZE = (long) Integer.MAX_VALUE * 200;
        size = size & 0x07FFFFFFFFFFFFFFFL;
        return ((size < MAX_AREA_SIZE) && (size >= 24) && ((size & 0x07) == 0));
    }

    /**
     * Reads an 8 byte long at the given position in the data area.
     */
    private final byte[] buf = new byte[8];

    private long readLongAt(long position) throws IOException {
        readByteArrayFrom(position, buf, 0, 8);
        return ByteArrayUtil.getLong(buf, 0);
    }

    /**
     * Performs a repair scan from the given pointer.  This is a recursive
     * algorithm that looks for at most 'n' number of repairs before giving
     * up.  Returns false if a repair path could not be found.
     */
    private boolean repairScan(final ArrayList<Object> areas_to_fix,
                               final long pointer, final long end_pointer,
                               final boolean scan_forward,
                               final int max_repairs) throws IOException {
        // Recurse end conditions;
        // If the end is reached, success!
        if (pointer == end_pointer) {
            return true;
        }
        // If max repairs exhausted, failure!
        if (pointer > end_pointer || max_repairs <= 0) {
            return false;
        }

        long pointer_to_head = scan_forward ? pointer : end_pointer - 8;

        // Does the pointer at least look right?
        long first_header = readLongAt(pointer_to_head) & 0x07FFFFFFFFFFFFFFFL;
        // If it's a valid boundary size, and the header points inside the
        // end boundary
        long max_bound_size = end_pointer - pointer;
        if (isValidBoundarySize(first_header) && first_header <= max_bound_size) {

            long pointer_to_tail = scan_forward ? (pointer + first_header) - 8 :
                    end_pointer - first_header;

            // If the end doesn't look okay,
            long end_area_pointer = pointer_to_tail;
            long end_header = readLongAt(end_area_pointer) & 0x07FFFFFFFFFFFFFFFL;
            boolean valid_end_header = (first_header == end_header);

            long scan_area_p1 = scan_forward ? (pointer + first_header) : pointer;
            long scan_area_p2 = scan_forward ? end_pointer : (end_pointer - first_header);

            if (!valid_end_header) {
                // First and ends are invalid, so lets first assume we make the end
                // valid and recurse,
                long area_p = scan_forward ? pointer_to_head : pointer_to_tail;
                areas_to_fix.add(area_p);
                areas_to_fix.add(first_header);

                boolean b = repairScan(areas_to_fix, scan_area_p1, scan_area_p2,
                        true, max_repairs - 1);
                // If success
                if (b) {
                    return true;
                }

                // If failure, take that repair off the top
                areas_to_fix.remove(areas_to_fix.size() - 1);
                areas_to_fix.remove(areas_to_fix.size() - 1);
                // And keep searching
            } else {
                // Looks okay, so keep going,

                // This really does the same thing as recursing through the scan area
                // however, we have to reduce the stack usage for large files which
                // makes this iterative solution necessary.  Basically, this looks for
                // the first broken area and reverts back to applying the recursive
                // algorithm on it.
                boolean something_broken = false;
                long previous1_scan_area_p1 = scan_area_p1;
                long previous2_scan_area_p1 = scan_area_p1;
                long previous3_scan_area_p1 = scan_area_p1;

                while (scan_area_p1 < scan_area_p2 && !something_broken) {
                    // Assume something broken,
                    something_broken = true;

                    // Does the pointer at least look right?
                    long scanning_header =
                            readLongAt(scan_area_p1) & 0x07FFFFFFFFFFFFFFFL;
                    long scan_max_bound_size = scan_area_p2 - scan_area_p1;
                    if (isValidBoundarySize(scanning_header) &&
                            scanning_header <= scan_max_bound_size) {
                        long scan_end_header =
                                readLongAt((scan_area_p1 + scanning_header) - 8)
                                        & 0x07FFFFFFFFFFFFFFFL;
                        if (scan_end_header == scanning_header) {
                            // Cycle the scanned areas
                            previous3_scan_area_p1 = previous2_scan_area_p1;
                            previous2_scan_area_p1 = previous1_scan_area_p1;
                            previous1_scan_area_p1 = scan_area_p1;

                            scan_area_p1 = (scan_area_p1 + scanning_header);
                            // Enough evidence that area is not broken, so continue scan
                            something_broken = false;
                        }
                    }
                }
                if (something_broken) {
                    // Back track to the last 3 scanned areas and perform a repair on
                    // this area.  This allows for the scan to have more choices on
                    // repair paths.
                    scan_area_p1 = previous3_scan_area_p1;
                }

                // The recursive scan on the (potentially) broken area.
                boolean b = repairScan(areas_to_fix, scan_area_p1, scan_area_p2,
                        true, max_repairs);
                if (b) {
                    // Repair succeeded!
                    return b;
                }

                // Repair didn't succeed so keep searching.
            }
        }

        // Try reversing the scan and see if that comes up with something valid.
        if (scan_forward) {
            boolean b = repairScan(areas_to_fix, pointer, end_pointer,
                    false, max_repairs);
            // Success
            if (b) {
                return b;
            }

        } else {
            return false;
        }

        // We guarenteed to be scan forward if we get here....

        // Facts: we know that the start and end pointers are invalid....

        // Search forward for something that looks like a boundary.  If we don't
        // find it, search backwards for something that looks like a boundary.

        final long max_size = end_pointer - pointer;
        for (long i = 16; i < max_size; i += 8) {

            long v = readLongAt(pointer + i) & 0x07FFFFFFFFFFFFFFFL;
            if (v == i + 8) {
                // This looks like a boundary, so try this...
                areas_to_fix.add(pointer);
                areas_to_fix.add(i + 8);
                boolean b = repairScan(areas_to_fix, pointer + i + 8, end_pointer,
                        true, max_repairs - 1);
                if (b) {
                    return true;
                }
                areas_to_fix.remove(areas_to_fix.size() - 1);
                areas_to_fix.remove(areas_to_fix.size() - 1);
            }

        }

        // Scan backwards....
        for (long i = max_size - 8 - 16; i >= 0; i -= 8) {

            long v = readLongAt(pointer + i) & 0x07FFFFFFFFFFFFFFFL;
            if (v == (max_size - i)) {
                // This looks like a boundary, so try this...
                areas_to_fix.add(pointer + i);
                areas_to_fix.add((max_size - i));
                boolean b = repairScan(areas_to_fix, pointer, pointer + i,
                        true, max_repairs - 1);
                if (b) {
                    return true;
                }
                areas_to_fix.remove(areas_to_fix.size() - 1);
                areas_to_fix.remove(areas_to_fix.size() - 1);
            }

        }

        // No luck, so simply set this as a final big area and return true.
        // NOTE: There are other tests possible here but I think what we have will
        //   find fixes for 99% of corruption cases.
        areas_to_fix.add(pointer);
        areas_to_fix.add(end_pointer - pointer);

        return true;

    }

    /**
     * Opens/scans the store looking for any errors with the layout.  If a
     * problem with the store is detected, it attempts to fix it.
     */
    public synchronized void openScanAndFix(UserTerminal terminal)
            throws IOException {

        internalOpen(read_only);

        terminal.println("- Store: " + toString());

        // If it's small, initialize to empty
        if (endOfDataAreaPointer() < DATA_AREA_OFFSET) {
            terminal.println("+ Store too small - initializing to empty.");
            initializeToEmpty();
            return;
        }

        byte[] read_buf = new byte[(int) BIN_AREA_OFFSET];
        readByteArrayFrom(0, read_buf, 0, read_buf.length);
        ByteArrayInputStream b_in = new ByteArrayInputStream(read_buf);
        DataInputStream din = new DataInputStream(b_in);

        int magic = din.readInt();
        if (magic != MAGIC) {
            terminal.println("! Store magic value not present - not fixable.");
            return;
        }
        int version = din.readInt();
        if (version != 1) {
            terminal.println("! Store version is invalid - not fixable.");
            return;
        }

        // Check the size
        long end_of_data_area = endOfDataAreaPointer();
        if (end_of_data_area < DATA_AREA_OFFSET + 16) {
            // Store size is too small.  There's nothing to be lost be simply
            // reinitializing it to a blank state.
            terminal.println(
                    "! Store is too small, reinitializing store to blank state.");
            initializeToEmpty();
            return;
        }

        // Do a recursive scan over the store.
        ArrayList<Object> repairs = new ArrayList<>();
        boolean b = repairScan(repairs, DATA_AREA_OFFSET, endOfDataAreaPointer(),
                true, 20);

        if (b) {
            if (repairs.size() == 0) {
                terminal.println("- Store areas are intact.");
            } else {
                terminal.println("+ " + (repairs.size() / 2) + " area repairs:");
                for (int i = 0; i < repairs.size(); i += 2) {
                    terminal.println("  Area pointer: " + repairs.get(i));
                    terminal.println("  Area size: " + repairs.get(i + 1));
                    long pointer = (Long) repairs.get(i);
                    long size = (Long) repairs.get(i + 1);
                    coalescArea(pointer, size);
                }
            }
        } else {
            terminal.println("- Store is not repairable!");
        }

        // Rebuild the free bins,
        free_bin_list = new long[BIN_ENTRIES + 1];
        for (int i = 0; i < BIN_ENTRIES + 1; ++i) {
            free_bin_list[i] = -1;
        }

        terminal.println("+ Rebuilding free bins.");
        long[] header = new long[2];
        // Scan for all free areas in the store.
        long pointer = DATA_AREA_OFFSET;
        while (pointer < end_of_data_area) {
            getAreaHeader(pointer, header);
            long area_size = (header[0] & 0x07FFFFFFFFFFFFFFFL);
            boolean is_free = ((header[0] & 0x08000000000000000L) != 0);

            if (is_free) {
                addToBinChain(pointer, area_size);
            }

            pointer += area_size;
        }

        // Update all the bins
        writeAllBins();

        terminal.println("- Store repair complete.");

        // Open the store for real,
        open();

    }

    /**
     * Performs an extensive lookup on all the tables in this store and sets a
     * number of properties in the given HashMap
     * (property name(String) -> property description(Object)).  This should be
     * used for store diagnostics.
     * <p>
     * Assume the store is open.
     */
    public synchronized void statsScan(HashMap<Object,Object> properties) throws IOException {

        long free_areas = 0;
        long free_total = 0;
        long allocated_areas = 0;
        long allocated_total = 0;

        final long end_of_data_area = endOfDataAreaPointer();

        long[] header = new long[2];
        // The first header
        long pointer = DATA_AREA_OFFSET;
        while (pointer < end_of_data_area) {
            getAreaHeader(pointer, header);
            long area_size = (header[0] & 0x07FFFFFFFFFFFFFFFL);

            if ((header[0] & 0x08000000000000000L) != 0) {
                ++free_areas;
                free_total += area_size;
            } else {
                ++allocated_areas;
                allocated_total += area_size;
            }

            pointer += area_size;
        }

        if (wilderness_pointer != -1) {
            getAreaHeader(wilderness_pointer, header);
            long wilderness_size = (header[0] & 0x07FFFFFFFFFFFFFFFL);
            properties.put("AbstractStore.wilderness_size",
                    wilderness_size);
        }

        properties.put("AbstractStore.end_of_data_area",
                end_of_data_area);
        properties.put("AbstractStore.free_areas",
                free_areas);
        properties.put("AbstractStore.free_total",
                free_total);
        properties.put("AbstractStore.allocated_areas",
                allocated_areas);
        properties.put("AbstractStore.allocated_total",
                allocated_total);


    }

    /**
     * Returns a List of Long objects that contain a complete list of all areas
     * in the store.  This is useful for checking if a given pointer is valid
     * or not.  The returned list is sorted from start area to end area.
     */
    public List<Object> getAllAreas() throws IOException {
        ArrayList<Object> list = new ArrayList<>();
        final long end_of_data_area = endOfDataAreaPointer();
        long[] header = new long[2];
        // The first header
        long pointer = DATA_AREA_OFFSET;
        while (pointer < end_of_data_area) {
            getAreaHeader(pointer, header);
            long area_size = (header[0] & 0x07FFFFFFFFFFFFFFFL);
            if ((header[0] & 0x08000000000000000L) == 0) {
                list.add(pointer);
            }
            pointer += area_size;
        }
        return list;
    }

    /**
     * Scans the area list, and any areas that aren't deleted and aren't found
     * in the given ArrayList are returned as leaked areas.  This is a useful
     * method for finding any leaks in the store.
     */
    public ArrayList findAllocatedAreasNotIn(ArrayList list) throws IOException {

        // Sort the list
        Collections.sort(list);

        // The list of leaked areas
        ArrayList leaked_areas = new ArrayList();

        int list_index = 0;

        // What area are we looking for?
        long looking_for = Long.MAX_VALUE;
        if (list_index < list.size()) {
            looking_for = (Long) list.get(list_index);
            ++list_index;
        }

        final long end_of_data_area = endOfDataAreaPointer();
        long[] header = new long[2];

        long pointer = DATA_AREA_OFFSET;
        while (pointer < end_of_data_area) {
            getAreaHeader(pointer, header);
            long area_size = (header[0] & 0x07FFFFFFFFFFFFFFFL);
            boolean area_free = (header[0] & 0x08000000000000000L) != 0;

            if (pointer == looking_for) {
                if (area_free) {
                    throw new IOException("Area (pointer = " + pointer +
                            ") is not allocated!");
                }
                // Update the 'looking_for' pointer
                if (list_index < list.size()) {
                    looking_for = (Long) list.get(list_index);
                    ++list_index;
                } else {
                    looking_for = Long.MAX_VALUE;
                }
            } else if (pointer > looking_for) {
                throw new IOException("Area (pointer = " + looking_for +
                        ") wasn't found in store!");
            } else {
                // An area that isn't in the list
                if (!area_free) {
                    // This is a leaked area.
                    // It isn't free and it isn't in the list
                    leaked_areas.add(pointer);
                }
            }

            pointer += area_size;
        }

        return leaked_areas;

    }

    /**
     * Returns the total allocated space since the file was openned.
     */
    public synchronized long totalAllocatedSinceStart() {
        return total_allocated_space;
    }

    /**
     * Returns the bin index that would be the minimum size to store the given
     * object.
     */
    private int minimumBinSizeIndex(long size) {
        int i = Arrays.binarySearch(BIN_SIZES, (int) size);
        if (i < 0) {
            i = -(i + 1);
        }
        return i;
    }

    /**
     * Internally opens the backing area.  If 'read_only' is true then the
     * store is openned in read only mode.
     */
    protected abstract void internalOpen(boolean read_only) throws IOException;

    /**
     * Internally closes the backing area.
     */
    protected abstract void internalClose() throws IOException;

    /**
     * Reads a byte from the given position in the file.
     */
    protected abstract int readByteFrom(long position) throws IOException;

    /**
     * Reads a byte array from the given position in the file.  Returns the
     * number of bytes read.
     */
    protected abstract int readByteArrayFrom(long position,
                                             byte[] buf, int off, int len) throws IOException;

    /**
     * Writes a byte to the given position in the file.
     */
    protected abstract void writeByteTo(long position, int b) throws IOException;

    /**
     * Writes a byte array to the given position in the file.
     */
    protected abstract void writeByteArrayTo(long position,
                                             byte[] buf, int off, int len) throws IOException;

    /**
     * Returns a pointer to the end of the current data area.
     */
    protected abstract long endOfDataAreaPointer() throws IOException;

    /**
     * Sets the size of the data area.
     */
    protected abstract void setDataAreaSize(long length) throws IOException;


    /**
     * WriteByteTo pass-through method.
     */
    private void writeByteToPT(long position, int b) throws IOException {
        writeByteTo(position, b);
    }

    /**
     * WriteByteArrayTo pass-through method.
     */
    private void writeByteArrayToPT(long position,
                                    byte[] buf, int off, int len) throws IOException {
        writeByteArrayTo(position, buf, off, len);
    }


    // ----------

    /**
     * Checks the pointer is valid.
     */
    protected void checkPointer(long pointer) throws IOException {
        if (pointer < DATA_AREA_OFFSET || pointer >= endOfDataAreaPointer()) {
            throw new IOException("Pointer out of range: " + DATA_AREA_OFFSET +
                    " > " + pointer + " > " + endOfDataAreaPointer());
        }
    }

    /**
     * A buffered work area we work with when reading/writing bin pointers from
     * the file header.
     */
    private final byte[] bin_area = new byte[128 * 8];

    /**
     * Reads the bins from the header information in the file.
     */
    protected void readBins() throws IOException {
        readByteArrayFrom(BIN_AREA_OFFSET,
                bin_area, 0, 128 * 8);
        ByteArrayInputStream bin = new ByteArrayInputStream(bin_area);
        DataInputStream in = new DataInputStream(bin);
        for (int i = 0; i < 128; ++i) {
            free_bin_list[i] = in.readLong();
        }
    }

    /**
     * Updates all bins to the data area header area.
     */
    protected void writeAllBins() throws IOException {
        int p = 0;
        for (int i = 0; i < 128; ++i, p += 8) {
            long val = free_bin_list[i];
            ByteArrayUtil.setLong(val, bin_area, p);
        }
        writeByteArrayToPT(BIN_AREA_OFFSET, bin_area, 0, 128 * 8);
    }

    /**
     * Updates the given bin index to the data area header area.
     */
    protected void writeBinIndex(int index) throws IOException {
        int p = index * 8;
        long val = free_bin_list[index];
        ByteArrayUtil.setLong(val, bin_area, p);
        writeByteArrayToPT(BIN_AREA_OFFSET + p, bin_area, p, 8);
    }

    protected final byte[] header_buf = new byte[16];

    /**
     * Sets the 'header' array with information from the header of the given
     * pointer.
     */
    protected void getAreaHeader(long pointer, long[] header) throws IOException {
        readByteArrayFrom(pointer, header_buf, 0, 16);
        header[0] = ByteArrayUtil.getLong(header_buf, 0);
        header[1] = ByteArrayUtil.getLong(header_buf, 8);
    }

    /**
     * Sets the 'header' array with information from the previous header to the
     * given pointer, and returns a pointer to the previous area.
     */
    protected long getPreviousAreaHeader(long pointer, long[] header)
            throws IOException {
        // If the pointer is the start of the file area
        if (pointer == DATA_AREA_OFFSET) {
            // Return a 0 sized block
            header[0] = 0;
            return -1;
        } else {
            readByteArrayFrom(pointer - 8, header_buf, 0, 8);
            long sz = ByteArrayUtil.getLong(header_buf, 0);
            sz = sz & 0x07FFFFFFFFFFFFFFFL;
            long previous_pointer = pointer - sz;
            readByteArrayFrom(previous_pointer, header_buf, 0, 8);
            header[0] = ByteArrayUtil.getLong(header_buf, 0);
            return previous_pointer;
        }
    }

    /**
     * Sets the 'header' array with information from the next header to the
     * given pointer, and returns a pointer to the next area.
     */
    protected long getNextAreaHeader(long pointer, long[] header)
            throws IOException {
        readByteArrayFrom(pointer, header_buf, 0, 8);
        long sz = ByteArrayUtil.getLong(header_buf, 0);
        sz = sz & 0x07FFFFFFFFFFFFFFFL;
        long next_pointer = pointer + sz;

        if (next_pointer >= endOfDataAreaPointer()) {
            // Return a 0 sized block
            header[0] = 0;
            return -1;
        }

        readByteArrayFrom(next_pointer, header_buf, 0, 8);
        header[0] = ByteArrayUtil.getLong(header_buf, 0);
        return next_pointer;
    }

    /**
     * Rebounds the given area with the given header information.  If
     * 'write_headers' is true, the header (header[0]) is changed.  Note that this
     * shouldn't be used to change the size of a chunk.
     */
    protected void reboundArea(long pointer, long[] header,
                               boolean write_headers) throws IOException {
        if (write_headers) {
            ByteArrayUtil.setLong(header[0], header_buf, 0);
            ByteArrayUtil.setLong(header[1], header_buf, 8);
            writeByteArrayToPT(pointer, header_buf, 0, 16);
        } else {
            ByteArrayUtil.setLong(header[1], header_buf, 8);
            writeByteArrayToPT(pointer + 8, header_buf, 8, 8);
        }
    }

    /**
     * Coalesc one or more areas into a larger area.  This alters the boundary
     * of the area to encompass the given size.
     */
    protected void coalescArea(long pointer, long size) throws IOException {

        ByteArrayUtil.setLong(size, header_buf, 0);

        // ISSUE: Boundary alteration is a moment when corruption could occur.
        //   There are two seeks and writes here and when we are setting the
        //   end points, there is a risk of failure.

        writeByteArrayToPT(pointer, header_buf, 0, 8);
        writeByteArrayToPT((pointer + size) - 8, header_buf, 0, 8);
    }

    /**
     * Expands the data area by at least the minimum size given.  Returns the
     * actual size the data area was expanded by.
     */
    protected long expandDataArea(long minimum_size) throws IOException {
        long end_of_data_area = endOfDataAreaPointer();

        // Round all sizes up to the nearest 8
        // We grow only by a small amount if the area is small, and a large amount
        // if the area is large.
        long over_grow = end_of_data_area / 64;
        long d = (over_grow & 0x07L);
        if (d != 0) {
            over_grow = over_grow + (8 - d);
        }
        over_grow = Math.min(over_grow, 262144L);
        if (over_grow < 1024) {
            over_grow = 1024;
        }

        long grow_by = minimum_size + over_grow;
        long new_file_length = end_of_data_area + grow_by;
        setDataAreaSize(new_file_length);
        return grow_by;
    }

    /**
     * Splits an area pointed to by 'pointer' at a new boundary point.
     */
    protected void splitArea(long pointer, long new_boundary) throws IOException {
        // Split the area pointed to by the pointer.
        readByteArrayFrom(pointer, header_buf, 0, 8);
        long cur_size = ByteArrayUtil.getLong(header_buf, 0) & 0x07FFFFFFFFFFFFFFFL;
        long left_size = new_boundary;
        long right_size = cur_size - new_boundary;

        if (right_size < 0) {
            throw new Error("right_size < 0");
        }

        ByteArrayUtil.setLong(left_size, header_buf, 0);
        ByteArrayUtil.setLong(right_size, header_buf, 8);

        // ISSUE: Boundary alteration is a moment when corruption could occur.
        //   There are three seeks and writes here and when we are setting the
        //   end points, there is a risk of failure.

        // First set the boundary
        writeByteArrayToPT((pointer + new_boundary) - 8, header_buf, 0, 16);
        // Now set the end points
        writeByteArrayToPT(pointer, header_buf, 0, 8);
        writeByteArrayToPT((pointer + cur_size) - 8, header_buf, 8, 8);
    }


    private final long[] header_info = new long[2];
    private final long[] header_info2 = new long[2];


    /**
     * Adds the given area to the bin represented by the bin_chain_index.
     */
    private void addToBinChain(long pointer, long size) throws IOException {

        checkPointer(pointer);

        // What bin would this area fit into?
        int bin_chain_index = minimumBinSizeIndex(size);

//    System.out.println("+ Adding to bin chain: " + pointer + " size: " + size);
//    System.out.println("+ Adding to index: " + bin_chain_index);

        long cur_pointer = free_bin_list[bin_chain_index];
        if (cur_pointer == -1) {
            // If the bin chain has no elements,
            header_info[0] = (size | 0x08000000000000000L);
            header_info[1] = -1;
            reboundArea(pointer, header_info, true);
            free_bin_list[bin_chain_index] = pointer;
            writeBinIndex(bin_chain_index);
        } else {
            boolean inserted = false;
            long last_pointer = -1;
            int searches = 0;
            while (cur_pointer != -1 && inserted == false) {
                // Get the current pointer
                getAreaHeader(cur_pointer, header_info);

                long header = header_info[0];
                long next = header_info[1];
                // Assert - the header must have deleted flag
                if ((header & 0x08000000000000000L) == 0) {
                    throw new Error("Assert failed - area not marked as deleted.  " +
                            "pos = " + cur_pointer +
                            " this = " + toString());
                }
                long area_size = header ^ 0x08000000000000000L;
                if (area_size >= size || searches >= 12) {
                    // Insert if the area size is >= than the size we are adding.
                    // Set the previous header to point to this
                    long previous = last_pointer;

                    // Set up the deleted area
                    header_info[0] = (size | 0x08000000000000000L);
                    header_info[1] = cur_pointer;
                    reboundArea(pointer, header_info, true);

                    if (last_pointer != -1) {
                        // Set the previous in the chain to point to the deleted area
                        getAreaHeader(previous, header_info);
                        header_info[1] = pointer;
                        reboundArea(previous, header_info, false);
                    } else {
                        // Otherwise set the head bin item
                        free_bin_list[bin_chain_index] = pointer;
                        writeBinIndex(bin_chain_index);
                    }

                    inserted = true;
                }
                last_pointer = cur_pointer;
                cur_pointer = next;
                ++searches;
            }

            // If we reach the end and we haven't inserted,
            if (!inserted) {
                // Set the new deleted area.
                header_info[0] = (size | 0x08000000000000000L);
                header_info[1] = -1;
                reboundArea(pointer, header_info, true);

                // Set the previous entry to this
                getAreaHeader(last_pointer, header_info);
                header_info[1] = pointer;
                reboundArea(last_pointer, header_info, false);

            }

        }

    }

    /**
     * Removes the given area from the bin chain.  This requires a search of the
     * bin chain for the given size.
     */
    private void removeFromBinChain(long pointer, long size) throws IOException {
        // What bin index should we be looking in?
        int bin_chain_index = minimumBinSizeIndex(size);

//    System.out.println("- Removing from bin chain " + pointer + " size " + size);
//    System.out.println("- Removing from index " + bin_chain_index);

        long previous_pointer = -1;
        long cur_pointer = free_bin_list[bin_chain_index];
        // Search this bin for the pointer
        // NOTE: This is an iterative search through the bin chain
        while (pointer != cur_pointer) {
            if (cur_pointer == -1) {
                throw new IOException("Area not found in bin chain!  " +
                        "pos = " + pointer + " store = " + toString());
            }
            // Move to the next in the chain
            getAreaHeader(cur_pointer, header_info);
            previous_pointer = cur_pointer;
            cur_pointer = header_info[1];
        }

        // Found the pointer, so remove it,
        if (previous_pointer == -1) {
            getAreaHeader(pointer, header_info);
            free_bin_list[bin_chain_index] = header_info[1];
            writeBinIndex(bin_chain_index);
        } else {
            getAreaHeader(previous_pointer, header_info2);
            getAreaHeader(pointer, header_info);
            header_info2[1] = header_info[1];
            reboundArea(previous_pointer, header_info2, false);
        }

    }

    /**
     * Crops the area to the given size.  This is used after an area is pulled
     * from a bin.  This method decides if it's worth reusing any space left over
     * and the end of the area.
     */
    private void cropArea(long pointer, long allocated_size) throws IOException {
        // Get the header info
        getAreaHeader(pointer, header_info);
        long header = header_info[0];
        // Can we recycle the difference in area size?
        final long free_area_size = header;
        // The difference between the size of the free area and the size
        // of the allocated area?
        final long size_difference = free_area_size - allocated_size;
        // If the difference is greater than 512 bytes, add the excess space to
        // a free bin.
        boolean is_wilderness = (pointer == wilderness_pointer);
        if ((is_wilderness && size_difference >= 32) || size_difference >= 512) {
            // Split the area into two areas.
            splitArea(pointer, allocated_size);

            long left_over_pointer = pointer + allocated_size;
            // Add this area to the bin chain
            addToBinChain(left_over_pointer, size_difference);

            // If pointer is the wilderness area, set this as the new wilderness
            if (is_wilderness ||
                    (left_over_pointer + size_difference) >= endOfDataAreaPointer()) {
                wilderness_pointer = left_over_pointer;
            }

        } else {
            // If pointer is the wilderness area, set wilderness to -1
            if (is_wilderness) {
                wilderness_pointer = -1;
            }
        }
    }

    /**
     * Allocates a block of memory from the backing area of the given size and
     * returns a pointer to that area.
     */
    private long alloc(long size) throws IOException {

        // Negative allocations are not allowed
        if (size < 0) {
            throw new IOException("Negative size allocation");
        }

        // Add 16 bytes for headers
        size = size + 16;
        // If size < 32, make size = 32
        if (size < 32) {
            size = 32;
        }

        // Round all sizes up to the nearest 8
        long d = size & 0x07L;
        if (d != 0) {
            size = size + (8 - d);
        }

        final long real_alloc_size = size;

        // Search the free bin list for the first bin that matches the given size.
        int bin_chain_index;
        if (size > MAX_BIN_SIZE) {
            bin_chain_index = BIN_ENTRIES;
        } else {
            int i = minimumBinSizeIndex(size);
            bin_chain_index = i;
        }

        // Search the bins until we find the first area that is the nearest fit to
        // the size requested.
        int found_bin_index = -1;
        long previous_pointer = -1;
        boolean first = true;
        for (int i = bin_chain_index;
             i < BIN_ENTRIES + 1 && found_bin_index == -1; ++i) {
            long cur_pointer = free_bin_list[i];
            if (cur_pointer != -1) {
                if (!first) {
                    // Pick this..
                    found_bin_index = i;
                    previous_pointer = -1;
                }
                // Search this bin for the first that's big enough.
                // We only search the first 12 entries in the bin before giving up.
                else {
                    long last_pointer = -1;
                    int searches = 0;
                    while (cur_pointer != -1 &&
                            found_bin_index == -1 &&
                            searches < 12) {
                        getAreaHeader(cur_pointer, header_info);
                        long area_size = (header_info[0] & 0x07FFFFFFFFFFFFFFFL);
                        // Is this area is greater or equal than the required size
                        // and is not the wilderness area, pick it.
                        if (cur_pointer != wilderness_pointer && area_size >= size) {
                            found_bin_index = i;
                            previous_pointer = last_pointer;
                        }
                        // Go to next in chain.
                        last_pointer = cur_pointer;
                        cur_pointer = header_info[1];
                        ++searches;
                    }
                }

            }
            first = false;
        }

        // If no area can be recycled,
        if (found_bin_index == -1) {

            // Allocate a new area of the given size.
            // If there is a wilderness, grow the wilderness area to the new size,
            long working_pointer;
            long size_to_grow;
            long current_area_size;
            if (wilderness_pointer != -1) {
                working_pointer = wilderness_pointer;
                getAreaHeader(wilderness_pointer, header_info);
                long wilderness_size = (header_info[0] & 0x07FFFFFFFFFFFFFFFL);
                // Remove this from the bins
                removeFromBinChain(working_pointer, wilderness_size);
                // For safety, we set wilderness_pointer to -1
                wilderness_pointer = -1;
                size_to_grow = size - wilderness_size;
                current_area_size = wilderness_size;
            } else {
                // wilderness_pointer == -1 so add to the end of the data area.
                working_pointer = endOfDataAreaPointer();
                size_to_grow = size;
                current_area_size = 0;
            }

            long expanded_size = 0;
            if (size_to_grow > 0) {
                // Expand the data area to the new size.
                expanded_size = expandDataArea(size_to_grow);
            }
            // Coalesc the new area to the given size
            coalescArea(working_pointer, current_area_size + expanded_size);
            // crop the area
            cropArea(working_pointer, size);

            // Add to the total allocated space
            total_allocated_space += real_alloc_size;

            return working_pointer;
        } else {

            // An area is taken from the bins,
            long free_area_pointer;
            // Remove this area from the bin chain and possibly add any excess space
            // left over to a new bin.
            if (previous_pointer == -1) {
                free_area_pointer = free_bin_list[found_bin_index];
                getAreaHeader(free_area_pointer, header_info);
                free_bin_list[found_bin_index] = header_info[1];
                writeBinIndex(found_bin_index);
            } else {
                getAreaHeader(previous_pointer, header_info2);
                free_area_pointer = header_info2[1];
                getAreaHeader(free_area_pointer, header_info);
                header_info2[1] = header_info[1];
                reboundArea(previous_pointer, header_info2, false);
            }

            // Reset the header of the recycled area.
            header_info[0] = (header_info[0] & 0x07FFFFFFFFFFFFFFFL);
            reboundArea(free_area_pointer, header_info, true);

            // Crop the area to the given size.
            cropArea(free_area_pointer, size);

            // Add to the total allocated space
            total_allocated_space += real_alloc_size;

            return free_area_pointer;
        }

    }

    /**
     * Frees a previously allocated area in the store.
     */
    private void free(long pointer) throws IOException {

        // Get the area header
        getAreaHeader(pointer, header_info);

        if ((header_info[0] & 0x08000000000000000L) != 0) {
            throw new IOException("Area already marked as unallocated.");
        }

        // If (pointer + size) reaches the end of the header area, set this as the
        // wilderness.
        boolean set_as_wilderness =
                ((pointer + header_info[0]) >= endOfDataAreaPointer());

        long r_pointer = pointer;
        final long freeing_area_size = header_info[0];
        long r_size = freeing_area_size;

        // Can this area coalesc?
        long left_pointer = getPreviousAreaHeader(pointer, header_info2);
        boolean coalesc = false;
        if ((header_info2[0] & 0x08000000000000000L) != 0) {
            // Yes, we can coalesc left
            long area_size = (header_info2[0] & 0x07FFFFFFFFFFFFFFFL);

            r_pointer = left_pointer;
            r_size = r_size + area_size;
            // Remove left area from the bin
            removeFromBinChain(left_pointer, area_size);
            coalesc = true;

        }

        if (!set_as_wilderness) {
            long right_pointer = getNextAreaHeader(pointer, header_info2);
            if ((header_info2[0] & 0x08000000000000000L) != 0) {
                // Yes, we can coalesc right
                long area_size = (header_info2[0] & 0x07FFFFFFFFFFFFFFFL);

                r_size = r_size + area_size;
                // Remove right from the bin
                removeFromBinChain(right_pointer, area_size);
                set_as_wilderness = (right_pointer == wilderness_pointer);
                coalesc = true;

            }
        }

        // If we are coalescing parent areas
        if (coalesc) {
            coalescArea(r_pointer, r_size);
        }

        // Add this new area to the bin chain,
        addToBinChain(r_pointer, r_size);

        // Do we set this as the wilderness?
        if (set_as_wilderness) {
            wilderness_pointer = r_pointer;
        }

        total_allocated_space -= freeing_area_size;

    }

    /**
     * Convenience for finding the size of an area.  If the area is deleted
     * throws an exception.
     */
    private long getAreaSize(final long pointer) throws IOException {
        final byte[] buf = new byte[8];
        readByteArrayFrom(pointer, buf, 0, 8);
        final long v = ByteArrayUtil.getLong(buf, 0);
        if ((v & 0x08000000000000000L) != 0) {
            throw new IOException("Area is deleted.");
        }
        return v - 16;
    }


    // ---------- Implemented from Store ----------

    public synchronized AreaWriter createArea(long size) throws IOException {
        long pointer = alloc(size);
        return new StoreAreaWriter(pointer, size);
    }

    public synchronized void deleteArea(long id) throws IOException {
        free(id);
    }

    public InputStream getAreaInputStream(long id) throws IOException {
        if (id == -1) {
            return new StoreAreaInputStream(FIXED_AREA_OFFSET, 64);
        } else {
            return new StoreAreaInputStream(id + 8, getAreaSize(id));
        }
    }

    public Area getArea(long id) throws IOException {
        // If this is the fixed area
        if (id == -1) {
            return new StoreArea(id, FIXED_AREA_OFFSET, 64);
        }
        // Otherwise must be a regular area
        else {
            return new StoreArea(id, id);
        }
    }

    public MutableArea getMutableArea(long id) throws IOException {
        // If this is the fixed area
        if (id == -1) {
            return new StoreMutableArea(id, FIXED_AREA_OFFSET, 64);
        }
        // Otherwise must be a regular area
        else {
            return new StoreMutableArea(id, id);
        }
    }

    public boolean lastCloseClean() {
        return !dirty_open;
    }

    // ---------- Inner classes ----------

    private class StoreAreaInputStream extends InputStream {

        private long pointer;
        private final long end_pointer;
        private long mark;

        public StoreAreaInputStream(long pointer, long max_size) {
            this.pointer = pointer;
            this.end_pointer = pointer + max_size;
            this.mark = -1;
        }

        public int read() throws IOException {
            if (pointer >= end_pointer) {
                return -1;
            }
            int b = readByteFrom(pointer);
            ++pointer;
            return b;
        }

        public int read(byte[] buf) throws IOException {
            return read(buf, 0, buf.length);
        }

        public int read(byte[] buf, int off, int len) throws IOException {
            // Is the end of the stream reached?
            if (pointer >= end_pointer) {
                return -1;
            }
            // How much can we read?
            int read_count = Math.min(len, (int) (end_pointer - pointer));
            int act_read_count;
            act_read_count = readByteArrayFrom(pointer, buf, off, read_count);
            if (act_read_count != read_count) {
                throw new IOException("act_read_count != read_count");
            }
            pointer += read_count;
            return read_count;
        }

        public long skip(long skip) throws IOException {
            long to_skip = Math.min(end_pointer - pointer, skip);
            pointer += to_skip;
            return to_skip;
        }

        public int available() throws IOException {
            return (int) (end_pointer - pointer);
        }

        public void close() throws IOException {
            // Do nothing
        }

        public void mark(int read_limit) {
            mark = pointer;
        }

        public void reset() throws IOException {
            pointer = mark;
        }

        public boolean markSupported() {
            return true;
        }

    }


    private class StoreArea implements Area {

        protected static final int BUFFER_SIZE = 8;

        protected final long id;
        protected final long start_pointer;
        protected final long end_pointer;
        protected long position;
        // A small buffer used when accessing the underlying data
        protected final byte[] buffer = new byte[BUFFER_SIZE];

        public StoreArea(final long id, final long pointer) throws IOException {
            // Check the pointer is within the bounds of the data area of the file
            checkPointer(pointer);

            readByteArrayFrom(pointer, buffer, 0, 8);
            final long v = ByteArrayUtil.getLong(buffer, 0);
            if ((v & 0x08000000000000000L) != 0) {
                throw new IOException("Store being constructed on deleted area.");
            }

            final long max_size = v - 16;
            this.id = id;
            this.start_pointer = pointer + 8;
            this.position = start_pointer;
            this.end_pointer = start_pointer + max_size;
        }

        public StoreArea(final long id, final long pointer, final long fixed_size)
                throws IOException {
            // Check the pointer is valid
            if (pointer != FIXED_AREA_OFFSET) {
                checkPointer(pointer);
            }

            this.id = id;
            this.start_pointer = pointer;
            this.position = start_pointer;
            this.end_pointer = start_pointer + fixed_size;
        }

        protected long checkPositionBounds(int diff) throws IOException {
            long new_pos = position + diff;
            if (new_pos > end_pointer) {
                throw new IOException("Position out of bounds. " +
                        " start=" + start_pointer +
                        " end=" + end_pointer +
                        " pos=" + position +
                        " new_pos=" + new_pos);
            }
            long old_pos = position;
            position = new_pos;
            return old_pos;
        }

        public long getID() {
            return id;
        }

        public int position() {
            return (int) (position - start_pointer);
        }

        public int capacity() {
            return (int) (end_pointer - start_pointer);
        }

        public void position(int position) throws IOException {
            long act_position = start_pointer + position;
            if (act_position >= 0 && act_position < end_pointer) {
                this.position = act_position;
                return;
            }
            throw new IOException("Moved position out of bounds.");
        }

        public void copyTo(AreaWriter destination_writer,
                           int size) throws IOException {
            // NOTE: Assuming 'destination' is a StoreArea, the temporary buffer
            // could be optimized away to a direct System.arraycopy.  However, this
            // function would need to be written as a lower level IO function.
            final int BUFFER_SIZE = 2048;
            byte[] buf = new byte[BUFFER_SIZE];
            int to_copy = Math.min(size, BUFFER_SIZE);

            while (to_copy > 0) {
                get(buf, 0, to_copy);
                destination_writer.put(buf, 0, to_copy);
                size -= to_copy;
                to_copy = Math.min(size, BUFFER_SIZE);
            }
        }

        public byte get() throws IOException {
            return (byte) readByteFrom(checkPositionBounds(1));
        }

        public void get(byte[] buf, int off, int len) throws IOException {
            readByteArrayFrom(checkPositionBounds(len), buf, off, len);
        }

        public short getShort() throws IOException {
            readByteArrayFrom(checkPositionBounds(2), buffer, 0, 2);
            return ByteArrayUtil.getShort(buffer, 0);
        }

        public int getInt() throws IOException {
            readByteArrayFrom(checkPositionBounds(4), buffer, 0, 4);
            return ByteArrayUtil.getInt(buffer, 0);
        }

        public long getLong() throws IOException {
            readByteArrayFrom(checkPositionBounds(8), buffer, 0, 8);
            return ByteArrayUtil.getLong(buffer, 0);
        }

        public char getChar() throws IOException {
            readByteArrayFrom(checkPositionBounds(2), buffer, 0, 2);
            return ByteArrayUtil.getChar(buffer, 0);
        }


        public String toString() {
            return "[Area start_pointer=" + start_pointer +
                    " end_pointer=" + end_pointer +
                    " position=" + position + "]";
        }

    }


    private class StoreMutableArea extends StoreArea implements MutableArea {

        public StoreMutableArea(final long id, final long pointer)
                throws IOException {
            super(id, pointer);
        }

        public StoreMutableArea(final long id, final long pointer,
                                final long fixed_size) throws IOException {
            super(id, pointer, fixed_size);
        }

        public void checkOut() throws IOException {
            // Currently, no-op
        }

        public void put(byte b) throws IOException {
            writeByteToPT(checkPositionBounds(1), b);
        }

        public void put(byte[] buf, int off, int len) throws IOException {
            writeByteArrayToPT(checkPositionBounds(len), buf, off, len);
        }

        public void put(byte[] buf) throws IOException {
            put(buf, 0, buf.length);
        }

        public void putShort(short s) throws IOException {
            ByteArrayUtil.setShort(s, buffer, 0);
            writeByteArrayToPT(checkPositionBounds(2), buffer, 0, 2);
        }

        public void putInt(int i) throws IOException {
            ByteArrayUtil.setInt(i, buffer, 0);
            writeByteArrayToPT(checkPositionBounds(4), buffer, 0, 4);
        }

        public void putLong(long l) throws IOException {
            ByteArrayUtil.setLong(l, buffer, 0);
            writeByteArrayToPT(checkPositionBounds(8), buffer, 0, 8);
        }

        public void putChar(char c) throws IOException {
            ByteArrayUtil.setChar(c, buffer, 0);
            writeByteArrayToPT(checkPositionBounds(2), buffer, 0, 2);
        }


        public String toString() {
            return "[MutableArea start_pointer=" + start_pointer +
                    " end_pointer=" + end_pointer +
                    " position=" + position + "]";
        }

    }


    /**
     * A simple OutputStream implementation that is on top of an AreaWriter
     * object.
     */
    static class AreaOutputStream extends OutputStream {

        private final AreaWriter writer;

        public AreaOutputStream(AreaWriter writer) {
            this.writer = writer;
        }

        public void write(int b) throws IOException {
            writer.put((byte) b);
        }

        public void write(byte[] buf) throws IOException {
            writer.put(buf, 0, buf.length);
        }

        public void write(byte[] buf, int off, int len) throws IOException {
            writer.put(buf, off, len);
        }

        public void flush() throws IOException {
            // do nothing
        }

        public void close() throws IOException {
            // do nothing
        }

    }


    private class StoreAreaWriter extends StoreMutableArea implements AreaWriter {

        public StoreAreaWriter(final long pointer, final long fixed_size)
                throws IOException {
            super(pointer, pointer + 8, fixed_size);
        }

        public OutputStream getOutputStream() {
            return new AreaOutputStream(this);
        }

        public void finish() throws IOException {
            // Currently, no-op
        }

    }


    // ---------- Static methods ----------

    /**
     * The default bin sizes in bytes.  The minimum size of a bin is 32 and the
     * maximum size is 2252832.
     */
    private final static int[] BIN_SIZES =
            {32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448, 480,
                    512, 544, 576, 608, 640, 672, 704, 736, 768, 800, 832, 864, 896, 928,
                    960, 992, 1024, 1056, 1088, 1120, 1152, 1184, 1216, 1248, 1280, 1312,
                    1344, 1376, 1408, 1440, 1472, 1504, 1536, 1568, 1600, 1632, 1664, 1696,
                    1728, 1760, 1792, 1824, 1856, 1888, 1920, 1952, 1984, 2016, 2048, 2080,
                    2144, 2208, 2272, 2336, 2400, 2464, 2528, 2592, 2656, 2720, 2784, 2848,
                    2912, 2976, 3040, 3104, 3168, 3232, 3296, 3360, 3424, 3488, 3552, 3616,
                    3680, 3744, 3808, 3872, 3936, 4000, 4064, 4128, 4384, 4640, 4896, 5152,
                    5408, 5664, 5920, 6176, 6432, 6688, 6944, 7200, 7456, 7712, 7968, 8224,
                    10272, 12320, 14368, 16416, 18464, 20512, 22560, 24608, 57376, 90144,
                    122912, 155680, 1204256, 2252832
            };

    protected final static int BIN_ENTRIES = BIN_SIZES.length;
    private final static int MAX_BIN_SIZE = BIN_SIZES[BIN_ENTRIES - 1];

}

