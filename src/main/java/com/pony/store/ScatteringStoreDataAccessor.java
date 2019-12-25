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

import java.util.ArrayList;
import java.io.*;

/**
 * An implementation of StoreDataAccessor that scatters the addressible
 * data resource across multiple files in the file system.  When one store
 * data resource reaches a certain threshold size, the content 'flows' over
 * to the next file.
 *
 * @author Tobias Downer
 */

public class ScatteringStoreDataAccessor implements StoreDataAccessor {

//  /**
//   * True if the data accessor is read only.
//   */
//  private boolean read_only;

    /**
     * The path of this store in the file system.
     */
    private final File path;

    /**
     * The name of the file in the file system minus the extension.
     */
    private final String file_name;

    /**
     * The extension of the first file in the sliced set.
     */
    private final String first_ext;

    /**
     * The maximum size a file slice can grow too before a new slice is created.
     */
    private final long max_slice_size;

    /**
     * The list of RandomAccessFile objects for each file that represents a
     * slice of the store.  (FileSlice objects)
     */
    private ArrayList slice_list;

    /**
     * The current actual physical size of the store data on disk.
     */
    private long true_file_length;

    /**
     * A lock when modifying the true_data_size, and slice_list.
     */
    private final Object lock = new Object();

    /**
     * Set when the store is openned.
     */
    private boolean open = false;


    /**
     * Constructs the store data accessor.
     */
    public ScatteringStoreDataAccessor(File path, String file_name,
                                       String first_ext, long max_slice_size) {
        slice_list = new ArrayList();
        this.path = path;
        this.file_name = file_name;
        this.first_ext = first_ext;
        this.max_slice_size = max_slice_size;
    }

    /**
     * Given a file, this will convert to a scattering file store with files
     * no larger than the maximum slice size.
     */
    public void convertToScatteringStore(File f) throws IOException {

        int BUFFER_SIZE = 65536;

        RandomAccessFile src = new RandomAccessFile(f, "rw");
        long file_size = f.length();
        long current_p = max_slice_size;
        long to_write = Math.min(file_size - current_p, max_slice_size);
        int write_to_part = 1;

        byte[] copy_buffer = new byte[BUFFER_SIZE];

        while (to_write > 0) {

            src.seek(current_p);

            File to_f = slicePartFile(write_to_part);
            if (to_f.exists()) {
                throw new IOException("Copy error, slice already exists.");
            }
            FileOutputStream to_raf = new FileOutputStream(to_f);

            while (to_write > 0) {
                int size_to_copy = (int) Math.min(BUFFER_SIZE, to_write);

                src.readFully(copy_buffer, 0, size_to_copy);
                to_raf.write(copy_buffer, 0, size_to_copy);

                current_p += size_to_copy;
                to_write -= size_to_copy;
            }

            to_raf.flush();
            to_raf.close();

            to_write = Math.min(file_size - current_p, max_slice_size);
            ++write_to_part;
        }

        // Truncate the source file
        if (file_size > max_slice_size) {
            src.seek(0);
            src.setLength(max_slice_size);
        }
        src.close();

    }

    /**
     * Given an index value, this will return a File object for the nth slice in
     * the file system.  For example, given '4' will return [file name].004,
     * given 1004 will return [file name].1004, etc.
     */
    private File slicePartFile(int i) {
        if (i == 0) {
            return new File(path, file_name + "." + first_ext);
        }
        StringBuffer fn = new StringBuffer();
        fn.append(file_name);
        fn.append(".");
        if (i < 10) {
            fn.append("00");
        } else if (i < 100) {
            fn.append("0");
        }
        fn.append(i);
        return new File(path, fn.toString());
    }

    /**
     * Counts the number of files in the file store that represent this store.
     */
    private int countStoreFiles() {
        int i = 0;
        File f = slicePartFile(i);
        while (f.exists()) {
            ++i;
            f = slicePartFile(i);
        }
        return i;
    }

    /**
     * Creates a StoreDataAccessor object for accessing a given slice.
     */
    private StoreDataAccessor createSliceDataAccessor(File file) {
        // Currently we only support an IOStoreDataAccessor object.
        return new IOStoreDataAccessor(file);
    }

    /**
     * Discovers the size of the data resource (doesn't require the file to be
     * open).
     */
    private long discoverSize() throws IOException {
        long running_total = 0;

        synchronized (lock) {
            // Does the file exist?
            int i = 0;
            File f = slicePartFile(i);
            while (f.exists()) {
                running_total += createSliceDataAccessor(f).getSize();

                ++i;
                f = slicePartFile(i);
            }
        }

        return running_total;
    }

    // ---------- Implemented from StoreDataAccessor ----------

    public void open(boolean read_only) throws IOException {
        long running_length;

        synchronized (lock) {
            slice_list = new ArrayList();

            // Does the file exist?
            File f = slicePartFile(0);
            boolean open_existing = f.exists();

            // If the file already exceeds the threshold and there isn't a secondary
            // file then we need to convert the file.
            if (open_existing && f.length() > max_slice_size) {
                File f2 = slicePartFile(1);
                if (f2.exists()) {
                    throw new IOException(
                            "File length exceeds maximum slice size setting.");
                }
                // We need to scatter the file.
                if (!read_only) {
                    convertToScatteringStore(f);
                } else {
                    throw new IOException(
                            "Unable to convert to a scattered store because read-only.");
                }
            }

            // Setup the first file slice
            FileSlice slice = new FileSlice();
            slice.data = createSliceDataAccessor(f);
            slice.data.open(read_only);

            slice_list.add(slice);
            running_length = slice.data.getSize();

            // If we are opening a store that exists already, there may be other
            // slices we need to setup.
            if (open_existing) {
                int i = 1;
                File slice_part = slicePartFile(i);
                while (slice_part.exists()) {
                    // Create the new slice information for this part of the file.
                    slice = new FileSlice();
                    slice.data = createSliceDataAccessor(slice_part);
                    slice.data.open(read_only);

                    slice_list.add(slice);
                    running_length += slice.data.getSize();

                    ++i;
                    slice_part = slicePartFile(i);
                }
            }

            true_file_length = running_length;

            open = true;
        }
    }

    public void close() throws IOException {
        synchronized (lock) {
            int sz = slice_list.size();
            for (int i = 0; i < sz; ++i) {
                FileSlice slice = (FileSlice) slice_list.get(i);
                slice.data.close();
            }
            slice_list = null;
            open = false;
        }
    }

    public boolean delete() {
        // The number of files
        int count_files = countStoreFiles();
        // Delete each file from back to front
        for (int i = count_files - 1; i >= 0; --i) {
            File f = slicePartFile(i);
            boolean delete_success = createSliceDataAccessor(f).delete();
            if (!delete_success) {
                return false;
            }
        }
        return true;
    }

    public boolean exists() {
        return slicePartFile(0).exists();
    }


    public void read(long position, byte[] buf, int off, int len)
            throws IOException {
        // Reads the array (potentially across multiple slices).
        while (len > 0) {
            int file_i = (int) (position / max_slice_size);
            long file_p = (position % max_slice_size);
            int file_len = (int) Math.min((long) len, max_slice_size - file_p);

            FileSlice slice;
            synchronized (lock) {
                // Return if out of bounds.
                if (file_i < 0 || file_i >= slice_list.size()) {
                    return;
                }
                slice = (FileSlice) slice_list.get(file_i);
            }
            slice.data.read(file_p, buf, off, file_len);

            position += file_len;
            off += file_len;
            len -= file_len;
        }
    }

    public void write(long position, byte[] buf, int off, int len)
            throws IOException {
        // Writes the array (potentially across multiple slices).
        while (len > 0) {
            int file_i = (int) (position / max_slice_size);
            long file_p = (position % max_slice_size);
            int file_len = (int) Math.min((long) len, max_slice_size - file_p);

            FileSlice slice;
            synchronized (lock) {
                // Return if out of bounds.
                if (file_i < 0 || file_i >= slice_list.size()) {
                    return;
                }
                slice = (FileSlice) slice_list.get(file_i);
            }
            slice.data.write(file_p, buf, off, file_len);

            position += file_len;
            off += file_len;
            len -= file_len;
        }
    }

    public void setSize(long length) throws IOException {
        synchronized (lock) {
            // The size we need to grow the data area
            long total_size_to_grow = length - true_file_length;
            // Assert that we aren't shrinking the data area size.
            if (total_size_to_grow < 0) {
                throw new IOException("Unable to make the data area size " +
                        "smaller for this type of store.");
            }

            while (total_size_to_grow > 0) {
                // Grow the last slice by this size
                int last = slice_list.size() - 1;
                FileSlice slice = (FileSlice) slice_list.get(last);
                final long old_slice_length = slice.data.getSize();
                long to_grow = Math.min(total_size_to_grow,
                        (max_slice_size - old_slice_length));

                // Flush the buffer and set the length of the file
                slice.data.setSize(old_slice_length + to_grow);
                // Synchronize the file change.  XP appears to defer a file size change
                // and it can result in errors if the JVM is terminated.
                slice.data.synch();

                total_size_to_grow -= to_grow;
                // Create a new empty slice if we need to extend the data area
                if (total_size_to_grow > 0) {
                    File slice_file = slicePartFile(last + 1);

                    slice = new FileSlice();
                    slice.data = createSliceDataAccessor(slice_file);
                    slice.data.open(false);

                    slice_list.add(slice);
                }
            }
            true_file_length = length;
        }

    }

    public long getSize() throws IOException {
        synchronized (lock) {
            if (open) {
                return true_file_length;
            } else {
                return discoverSize();
            }
        }
    }

    public void synch() throws IOException {
        synchronized (lock) {
            int sz = slice_list.size();
            for (int i = 0; i < sz; ++i) {
                FileSlice slice = (FileSlice) slice_list.get(i);
                slice.data.synch();
            }
        }
    }

    // ---------- Inner classes ----------

    /**
     * An object that contains information about a file slice.  The information
     * includes the name of the file, the RandomAccessFile that represents the
     * slice, and the size of the file.
     */
    private static class FileSlice {

        StoreDataAccessor data;

    }

}

