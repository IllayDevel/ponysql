/*
 * Pony SQL Database ( http://i-devel.ru )
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
import java.util.Comparator;
import java.util.Arrays;
import java.io.IOException;
import java.io.File;

import com.pony.debug.DebugLogger;

/**
 * A paged random access buffer manager that caches access between a Store and
 * the underlying filesystem and that also handles check point logging and
 * crash recovery (via a JournalledSystem object).
 *
 * @author Tobias Downer
 */

public class LoggingBufferManager {

    /**
     * Set to true for extra assertions.
     */
    private static final boolean PARANOID_CHECKS = false;

    /**
     * A timer that represents the T value in buffer pages.
     */
    private long current_T;

    /**
     * The number of pages in this buffer.
     */
    private int current_page_count;

    /**
     * The list of all pages.
     */
    private final ArrayList page_list;

    /**
     * A lock used when accessing the current_T, page_list and current_page_count
     * members.
     */
    private final Object T_lock = new Object();

    /**
     * A hash map of all pages currently in memory keyed by store_id and page
     * number.
     * NOTE: This MUST be final for the 'fetchPage' method to be safe.
     */
    private final BMPage[] page_map;

    /**
     * The JournalledSystem object that handles journalling of all data.
     */
    private final JournalledSystem journalled_system;

    /**
     * The maximum number of pages that should be kept in memory before pages
     * are written out to disk.
     */
    private final int max_pages;

    /**
     * The size of each page.
     */
    private final int page_size;

    // ---------- Write locks ----------

    /**
     * Set to true when a 'setCheckPoint' is in progress.
     */
    private boolean check_point_in_progress;

    /**
     * The number of write locks currently on the buffer.  Any number of write
     * locks can be obtained, however a 'setCheckpoint' can only be achieved
     * when there are no write operations in progress.
     */
    private int write_lock_count;

    /**
     * A mutex for when modifying the write lock information.
     */
    private final Object write_lock = new Object();


//  /**
//   * The number of cache hits.
//   */
//  private long cache_hit_count;
//
//  /**
//   * The number of cache misses.
//   */
//  private long cache_miss_count;


    /**
     * Constructs the manager.
     */
    public LoggingBufferManager(File journal_path, boolean read_only,
                                int max_pages, int page_size,
                                StoreDataAccessorFactory sda_factory,
                                DebugLogger debug, boolean enable_logging) {
        this.max_pages = max_pages;
        this.page_size = page_size;

        check_point_in_progress = false;
        write_lock_count = 0;

        current_T = 0;
        page_list = new ArrayList();
        page_map = new BMPage[257];
        int unique_id_seq = 0;

        journalled_system = new JournalledSystem(journal_path, read_only,
                page_size, sda_factory, debug, enable_logging);
    }

    /**
     * Constructs the manager with a scattering store implementation that
     * converts the resource to a file in the given path.
     */
    public LoggingBufferManager(final File resource_path,
                                final File journal_path, final boolean read_only, final int max_pages,
                                final int page_size, final String file_ext, final long max_slice_size,
                                DebugLogger debug, boolean enable_logging) {
        this(journal_path, read_only, max_pages, page_size,
                resource_name -> new ScatteringStoreDataAccessor(resource_path, resource_name,
                        file_ext, max_slice_size), debug, enable_logging);
    }

    /**
     * Starts the buffer manager.
     */
    public void start() throws IOException {
        journalled_system.start();
    }

    /**
     * Stops the buffer manager.
     */
    public void stop() throws IOException {
        journalled_system.stop();
    }

    // ----------

    /**
     * Creates a new resource.
     */
    JournalledResource createResource(String resource_name) {
        return journalled_system.createResource(resource_name);
    }

    /**
     * Obtains a write lock on the buffer.  This will block if a 'setCheckPoint'
     * is in progress, otherwise it will always succeed.
     */
    public void lockForWrite() throws InterruptedException {
        synchronized (write_lock) {
            while (check_point_in_progress) {
                write_lock.wait();
            }
            ++write_lock_count;
        }
    }

    /**
     * Releases a write lock on the buffer.  This MUST be called if the
     * 'lockForWrite' method is called.  This should be called from a 'finally'
     * clause.
     */
    public void unlockForWrite() {
        synchronized (write_lock) {
            --write_lock_count;
            write_lock.notifyAll();
        }
    }

    /**
     * Sets a check point in the log.  This logs a point in which a recovery
     * process should at least be able to be rebuild back to.  This will block
     * if there are any write locks.
     * <p>
     * Some things to keep in mind when using this.  You must ensure that no
     * writes can occur while this operation is occuring.  Typically this will
     * happen at the end of a commit but you need to ensure that nothing can
     * happen in the background, such as records being deleted or items being
     * inserted.  It is required that the 'no write' restriction is enforced at
     * a high level.  If care is not taken then the image written will not be
     * clean and if a crash occurs the image that is recovered will not be
     * stable.
     */
    public void setCheckPoint(boolean flush_journals)
            throws IOException, InterruptedException {

        // Wait until the writes have finished, and then set the
        // 'check_point_in_progress' boolean.
        synchronized (write_lock) {
            while (write_lock_count > 0) {
                write_lock.wait();
            }
            check_point_in_progress = true;
        }

        try {
//      System.out.println("SET CHECKPOINT");
            synchronized (page_map) {
                // Flush all the pages out to the log.
                for (int i = 0; i < page_map.length; ++i) {
                    BMPage page = page_map[i];
                    BMPage prev = null;

                    while (page != null) {
                        boolean deleted_hash = false;
                        synchronized (page) {
                            // Flush the page (will only actually flush if there are changes)
                            page.flush();

                            // Remove this page if it is no longer in use
                            if (page.notInUse()) {
                                deleted_hash = true;
                                if (prev == null) {
                                    page_map[i] = page.hash_next;
                                } else {
                                    prev.hash_next = page.hash_next;
                                }
                            }

                        }
                        // Go to next page in hash chain
                        if (!deleted_hash) {
                            prev = page;
                        }
                        page = page.hash_next;
                    }
                }
            }

            journalled_system.setCheckPoint(flush_journals);

        } finally {
            // Make sure we unset the 'check_point_in_progress' boolean and notify
            // any blockers.
            synchronized (write_lock) {
                check_point_in_progress = false;
                write_lock.notifyAll();
            }
        }

    }


    /**
     * Called when a new page is created.
     */
    private void pageCreated(final BMPage page) throws IOException {
        synchronized (T_lock) {

            if (PARANOID_CHECKS) {
                int i = page_list.indexOf(page);
                if (i != -1) {
                    BMPage f = (BMPage) page_list.get(i);
                    if (f == page) {
                        throw new Error("Same page added multiple times.");
                    }
                    if (f != null) {
                        throw new Error("Duplicate pages.");
                    }
                }
            }

            page.t = current_T;
            ++current_T;

            ++current_page_count;
            page_list.add(page);

            // Below is the page purge algorithm.  If the maximum number of pages
            // has been created we sort the page list weighting each page by time
            // since last accessed and total number of accesses and clear the bottom
            // 20% of this list.

            // Check if we should purge old pages and purge some if we do...
            if (current_page_count > max_pages) {
                // Purge 20% of the cache
                // Sort the pages by the current formula,
                //  ( 1 / page_access_count ) * (current_t - page_t)
                // Further, if the page has written data then we multiply by 0.75.
                // This scales down page writes so they have a better chance of
                // surviving in the cache than page writes.
                Object[] pages = page_list.toArray();
                Arrays.sort(pages, PAGE_CACHE_COMPARATOR);

                int purge_size = Math.max((int) (pages.length * 0.20f), 2);
                for (int i = 0; i < purge_size; ++i) {
                    BMPage dpage = (BMPage) pages[pages.length - (i + 1)];
                    synchronized (dpage) {
                        dpage.dispose();
                    }
                }

                // Remove all the elements from page_list and set it with the sorted
                // list (minus the elements we removed).
                page_list.clear();
                page_list.addAll(Arrays.asList(pages).subList(0, pages.length - purge_size));

                current_page_count -= purge_size;

            }
        }
    }

    /**
     * Called when a page is accessed.
     */
    private void pageAccessed(BMPage page) {
        synchronized (T_lock) {
            page.t = current_T;
            ++current_T;
            ++page.access_count;
        }
    }

    /**
     * Calculates a hash code given an id value and a page_number value.
     */
    private static int calcHashCode(long id, long page_number) {
        return (int) ((id << 6) + (page_number * ((id + 25) << 2)));
    }

    /**
     * Fetches and returns a page from a store.  Pages may be cached.  If the
     * page is not available in the cache then a new BMPage object is created
     * for the page requested.
     */
    private BMPage fetchPage(JournalledResource data,
                             final long page_number) throws IOException {
        final long id = data.getID();

        BMPage prev_page = null;
        boolean new_page = false;
        BMPage page;

        synchronized (page_map) {
            // Generate the hash code for this page.
            final int p = (calcHashCode(id, page_number) & 0x07FFFFFFF) %
                    page_map.length;
            // Search for this page in the hash
            page = page_map[p];
            while (page != null && !page.isPage(id, page_number)) {
                prev_page = page;
                page = page.hash_next;
            }

            // Page isn't found so create it and add to the cache
            if (page == null) {
                page = new BMPage(data, page_number, page_size);
                // Add this page to the map
                page.hash_next = page_map[p];
                page_map[p] = page;
            } else {
                // Move this page to the head if it's not already at the head.
                if (prev_page != null) {
                    prev_page.hash_next = page.hash_next;
                    page.hash_next = page_map[p];
                    page_map[p] = page;
                }
            }

            synchronized (page) {
                // If page not in use then it must be newly setup, so add a
                // reference.
                if (page.notInUse()) {
                    page.reset();
                    new_page = true;
                    page.referenceAdd();
                }
                // Add a reference for this fetch
                page.referenceAdd();
            }

        }

        // If the page is new,
        if (new_page) {
            pageCreated(page);
        } else {
            pageAccessed(page);
        }

        // Return the page.
        return page;

    }


    // ------
    // Buffered access methods.  These are all thread safe methods.  When a page
    // is accessed the page is synchronized so no 2 or more operations can
    // read/write from the page at the same time.  An operation can read/write to
    // different pages at the same time, however, and this requires thread safety
    // at a lower level (in the JournalledResource implementation).
    // ------

    int readByteFrom(JournalledResource data, long position) throws IOException {
        final long page_number = position / page_size;
        int v;

        BMPage page = fetchPage(data, page_number);
        synchronized (page) {
            try {
                page.initialize();
                v = ((int) page.read((int) (position % page_size))) & 0x0FF;
            } finally {
                page.dispose();
            }
        }

        return v;
    }

    int readByteArrayFrom(JournalledResource data,
                          long position, byte[] buf, int off, int len) throws IOException {

        final int orig_len = len;
        long page_number = position / page_size;
        int start_offset = (int) (position % page_size);
        int to_read = Math.min(len, page_size - start_offset);

        BMPage page = fetchPage(data, page_number);
        synchronized (page) {
            try {
                page.initialize();
                page.read(start_offset, buf, off, to_read);
            } finally {
                page.dispose();
            }
        }

        len -= to_read;
        while (len > 0) {
            off += to_read;
            position += to_read;
            ++page_number;
            to_read = Math.min(len, page_size);

            page = fetchPage(data, page_number);
            synchronized (page) {
                try {
                    page.initialize();
                    page.read(0, buf, off, to_read);
                } finally {
                    page.dispose();
                }
            }
            len -= to_read;
        }

        return orig_len;
    }

    void writeByteTo(JournalledResource data,
                     long position, int b) throws IOException {

        if (PARANOID_CHECKS) {
            synchronized (write_lock) {
                if (write_lock_count == 0) {
                    System.out.println("Write without a lock!");
                    new Error().printStackTrace();
                }
            }
        }

        final long page_number = position / page_size;

        BMPage page = fetchPage(data, page_number);
        synchronized (page) {
            try {
                page.initialize();
                page.write((int) (position % page_size), (byte) b);
            } finally {
                page.dispose();
            }
        }
    }

    void writeByteArrayTo(JournalledResource data,
                          long position, byte[] buf, int off, int len) throws IOException {

        if (PARANOID_CHECKS) {
            synchronized (write_lock) {
                if (write_lock_count == 0) {
                    System.out.println("Write without a lock!");
                    new Error().printStackTrace();
                }
            }
        }

        long page_number = position / page_size;
        int start_offset = (int) (position % page_size);
        int to_write = Math.min(len, page_size - start_offset);

        BMPage page = fetchPage(data, page_number);
        synchronized (page) {
            try {
                page.initialize();
                page.write(start_offset, buf, off, to_write);
            } finally {
                page.dispose();
            }
        }
        len -= to_write;

        while (len > 0) {
            off += to_write;
            position += to_write;
            ++page_number;
            to_write = Math.min(len, page_size);

            page = fetchPage(data, page_number);
            synchronized (page) {
                try {
                    page.initialize();
                    page.write(0, buf, off, to_write);
                } finally {
                    page.dispose();
                }
            }
            len -= to_write;
        }

    }

    void setDataAreaSize(JournalledResource data,
                         long new_size) throws IOException {
        data.setSize(new_size);
    }

    long getDataAreaSize(JournalledResource data) throws IOException {
        return data.getSize();
    }

    void close(JournalledResource data) throws IOException {
        long id = data.getID();
        // Flush all changes made to the resource then close.
        synchronized (page_map) {
//      System.out.println("Looking for id: " + id);
            // Flush all the pages out to the log.
            // This scans the entire hash for values and could be an expensive
            // operation.  Fortunately 'close' isn't used all that often.
            for (int i = 0; i < page_map.length; ++i) {
                BMPage page = page_map[i];
                BMPage prev = null;

                while (page != null) {
                    boolean deleted_hash = false;
                    if (page.getID() == id) {
//            System.out.println("Found page id: " + page.getID());
                        synchronized (page) {
                            // Flush the page (will only actually flush if there are changes)
                            page.flush();

                            // Remove this page if it is no longer in use
                            if (page.notInUse()) {
                                deleted_hash = true;
                                if (prev == null) {
                                    page_map[i] = page.hash_next;
                                } else {
                                    prev.hash_next = page.hash_next;
                                }
                            }
                        }

                    }

                    // Go to next page in hash chain
                    if (!deleted_hash) {
                        prev = page;
                    }
                    page = page.hash_next;

                }
            }
        }

        data.close();
    }


    // ---------- Inner classes ----------

    /**
     * A page from a store that is currently being cached in memory.  This is
     * also an element in the cache.
     */
    private static final class BMPage {

        /**
         * The StoreDataAccessor that the page content is part of.
         */
        private final JournalledResource data;

        /**
         * The page number.
         */
        private final long page;

        /**
         * The size of the page.
         */
        private final int page_size;


        /**
         * The buffer that contains the data for this page.
         */
        private byte[] buffer;

        /**
         * True if this page is initialized.
         */
        private boolean initialized;


        /**
         * A reference to the next page with this hash key.
         */
        BMPage hash_next;


        /**
         * The time this page was last accessed.  This value is reset each time
         * the page is requested.
         */
        long t;

        /**
         * The number of times this page has been accessed since it was created.
         */
        int access_count;


        /**
         * The first position in the buffer that was last written.
         */
        private int first_write_position;

        /**
         * The last position in the buffer that was last written.
         */
        private int last_write_position;

        /**
         * The number of references on this page.
         */
        private int reference_count;


        /**
         * Constructs the page.
         */
        BMPage(JournalledResource data, long page, int page_size) {
            this.data = data;
            this.page = page;
            this.reference_count = 0;
            this.page_size = page_size;
            reset();
        }

        /**
         * Resets this object.
         */
        void reset() {
            // Assert that this is 0
            if (reference_count != 0) {
                throw new Error("reset when 'reference_count' is != 0 ( = " +
                        reference_count + " )");
            }
            this.initialized = false;
            this.t = 0;
            this.access_count = 0;
        }

        /**
         * Returns the id of the JournalledResource that is being buffered.
         */
        long getID() {
            return data.getID();
        }

        /**
         * Adds 1 to the reference counter on this page.
         */
        void referenceAdd() {
            ++reference_count;
        }

        /**
         * Removes 1 from the reference counter on this page.
         */
        private void referenceRemove() {
            if (reference_count <= 0) {
                throw new Error("Too many reference remove.");
            }
            --reference_count;
        }

        /**
         * Returns true if this PageBuffer is not in use (has 0 reference count and
         * is not inialized.
         */
        boolean notInUse() {
            return reference_count == 0;
//      return (reference_count <= 0 && !initialized);
        }

        /**
         * Returns true if this page matches the given id/page_number.
         */
        boolean isPage(long in_id, long in_page) {
            return (getID() == in_id &&
                    page == in_page);
        }

        /**
         * Reads the current page content into memory.  This may read from the
         * data files or from a log.
         */
        private void readPageContent(
                long page_number, byte[] buf, int pos) throws IOException {
            if (pos != 0) {
                throw new Error("Assert failed: pos != 0");
            }
            // Read from the resource
            data.read(page_number, buf, pos);
        }

        /**
         * Flushes this page out to disk, but does not remove from memory.  In a
         * logging system this will flush the changes out to a log.
         */
        void flush() throws IOException {
            if (initialized) {
                if (last_write_position > -1) {
                    // Write to the store data.
                    data.write(page, buffer, first_write_position,
                            last_write_position - first_write_position);
//          System.out.println("FLUSH " + data + " off = " + first_write_position +
//                             " len = " + (last_write_position - first_write_position));
                }
                first_write_position = Integer.MAX_VALUE;
                last_write_position = -1;
            }
        }

        /**
         * Initializes the page buffer.  If the buffer is already initialized then
         * we just return.  If it's not initialized we set up any internal
         * structures that are required to be set up for access to this page.
         */
        void initialize() throws IOException {
            if (!initialized) {

                try {

                    // Create the buffer to contain the page in memory
                    buffer = new byte[page_size];
                    // Read the page.  This will either read the page from the backing
                    // store or from a log.
                    readPageContent(page, buffer, 0);
                    initialized = true;

//          access_count = 0;
                    first_write_position = Integer.MAX_VALUE;
                    last_write_position = -1;

                } catch (IOException e) {
                    // This makes debugging a little clearer if 'readPageContent' fails.
                    // When 'readPageContent' fails, the dispose method fails also.
                    System.out.println("IO Error during page initialize: " + e.getMessage());
                    e.printStackTrace();
                    throw e;
                }

            }
        }

        /**
         * Disposes of the page buffer if it can be disposed (there are no
         * references to the page and the page is initialized).  When disposed the
         * memory used by the page is reclaimed and the content is written out to
         * disk.
         */
        void dispose() throws IOException {
            referenceRemove();
            if (reference_count == 0) {
                if (initialized) {

                    // Flushes the page from memory.  This will write the page out to the
                    // log.
                    flush();

                    // Page is no longer initialized.
                    initialized = false;
                    // Clear the buffer from memory.
                    buffer = null;

                } else {
                    // This happens if initialization failed.  If this case we don't
                    // flush out the changes, but we do allow the page to be disposed
                    // in the normal way.
                    // Note that any exception generated by the initialization failure
                    // will propogate correctly.
                    buffer = null;
//          throw new RuntimeException(
//                "Assertion failed: tried to dispose an uninitialized page.");
                }
            }
        }


        /**
         * Reads a single byte from the cached page from memory.
         */
        byte read(int pos) {
            return buffer[pos];
        }

        /**
         * Reads a part of this page into the cached page from memory.
         */
        void read(int pos, byte[] buf, int off, int len) {
            System.arraycopy(buffer, pos, buf, off, len);
        }

        /**
         * Writes a single byte to the page in memory.
         */
        void write(int pos, byte v) {
            first_write_position = Math.min(pos, first_write_position);
            last_write_position = Math.max(pos + 1, last_write_position);

            buffer[pos] = v;
        }

        /**
         * Writes to the given part of the page in memory.
         */
        void write(int pos, byte[] buf, int off, int len) {
            first_write_position = Math.min(pos, first_write_position);
            last_write_position = Math.max(pos + len, last_write_position);

            System.arraycopy(buf, off, buffer, pos, len);
        }

        public boolean equals(Object ob) {
            BMPage dest_page = (BMPage) ob;
            return isPage(dest_page.getID(), dest_page.page);
        }

    }

    /**
     * A data resource that is being buffered.
     */
    private static class BResource {

        /**
         * The id assigned to the resource.
         */
        private final long id;

        /**
         * The unique name of the resource within the store.
         */
        private final String name;

        /**
         * Constructs the resource.
         */
        BResource(long id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * Returns the id assigned to this resource.
         */
        long getID() {
            return id;
        }

        /**
         * Returns the name of this resource.
         */
        String getName() {
            return name;
        }

    }

    /**
     * A Comparator used to sort cache entries.
     */
    private final Comparator PAGE_CACHE_COMPARATOR = new Comparator() {

        /**
         * The calculation for finding the 'weight' of a page in the cache.  A
         * heavier page is sorted lower and is therefore cleared from the cache
         * faster.
         */
        private float pageEnumValue(BMPage page) {
            // We fix the access counter so it can not exceed 10000 accesses.  I'm
            // a little unsure if we should put this constant in the equation but it
            // ensures that some old but highly accessed page will not stay in the
            // cache forever.
            final long bounded_page_count = Math.min(page.access_count, 10000);
            final float v = (1f / bounded_page_count) * (current_T - page.t);
            return v;
        }

        public int compare(Object ob1, Object ob2) {
            float v1 = pageEnumValue((BMPage) ob1);
            float v2 = pageEnumValue((BMPage) ob2);
            if (v1 > v2) {
                return 1;
            } else if (v1 < v2) {
                return -1;
            }
            return 0;
        }

    };

    /**
     * A factory interface for creating StoreDataAccessor objects from resource
     * names.
     */
    public interface StoreDataAccessorFactory {

        /**
         * Returns a StoreDataAccessor object for the given resource name.
         */
        StoreDataAccessor createStoreDataAccessor(String resource_name);

    }


}

