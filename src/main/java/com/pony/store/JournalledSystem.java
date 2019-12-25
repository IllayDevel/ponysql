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

import java.io.*;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.pony.debug.DebugLogger;
import com.pony.debug.Lvl;
import com.pony.util.ByteArrayUtil;
import com.pony.store.LoggingBufferManager.StoreDataAccessorFactory;

/**
 * Manages a journalling data store management system.  All operations are
 * written out to a log that can be easily recovered from if a crash occurs.
 *
 * @author Tobias Downer
 */

class JournalledSystem {

    /**
     * Set to true for logging behaviour.
     */
    private final boolean ENABLE_LOGGING;

    /**
     * The path to the journal files.
     */
    private final File journal_path;

    /**
     * If the journal system is in read only mode.
     */
    private final boolean read_only;

    /**
     * The page size.
     */
    private final int page_size;

    /**
     * The map of all resources that are available.  (resource_name -> Resource)
     */
    private HashMap all_resources;

    /**
     * The unique sequence id counter for this session.
     */
    private long seq_id;

    /**
     * The archive of journal files currently pending (JournalFile).
     */
    private final ArrayList journal_archives;

    /**
     * The current top journal file.
     */
    private JournalFile top_journal_file;

    /**
     * The current journal file number.
     */
    private long journal_number;

    /**
     * A factory that creates StoreDataAccessor objects used to access the
     * resource with the given name.
     */
    private final StoreDataAccessorFactory sda_factory;

    /**
     * Mutex when accessing the top journal file.
     */
    private final Object top_journal_lock = new Object();

    /**
     * A thread that runs in the background and persists information that is in
     * the journal.
     */
    private JournalingThread journaling_thread;

    /**
     * A debug log to output information to.
     */
    private final DebugLogger debug;


    JournalledSystem(File journal_path, boolean read_only, int page_size,
                     StoreDataAccessorFactory sda_factory, DebugLogger debug,
                     boolean enable_logging) {
        this.journal_path = journal_path;
        this.read_only = read_only;
        this.page_size = page_size;
        this.sda_factory = sda_factory;
        all_resources = new HashMap();
        journal_number = 0;
        journal_archives = new ArrayList();
        this.debug = debug;
        this.ENABLE_LOGGING = enable_logging;
    }


    /**
     * Returns a journal file name with the given number.  The journal number
     * must be between 10 and 63
     */
    private static String getJournalFileName(int number) {
        if (number < 10 || number > 73) {
            throw new Error("Journal file name out of range.");
        }
        return "jnl" + number;
    }

    // Lock used during initialization
    private final Object init_lock = new Object();

    /**
     * Starts the journal system.
     */
    void start() throws IOException {
        if (ENABLE_LOGGING) {
            synchronized (init_lock) {
                if (journaling_thread == null) {
                    // Start the background journaling thread,
                    journaling_thread = new JournalingThread();
                    journaling_thread.start();
                    // Scan for any changes and make the changes.
                    rollForwardRecover();
                    if (!read_only) {
                        // Create a new top journal file
                        newTopJournalFile();
                    }
                } else {
                    throw new Error("Assertion failed - already started.");
                }
            }
        }
    }

    /**
     * Stops the journal system.  This will persist any pending changes up to the
     * last check point and then finish.
     */
    void stop() throws IOException {
        if (ENABLE_LOGGING) {
            synchronized (init_lock) {
                if (journaling_thread != null) {
                    // Stop the journal thread
                    journaling_thread.persistArchives(0);
                    journaling_thread.finish();
                    journaling_thread.waitUntilFinished();
                    journaling_thread = null;
                } else {
                    throw new Error("Assertion failed - already stopped.");
                }
            }

            if (!read_only) {
                // Close any remaining journals and roll forward recover (shouldn't
                // actually be necessary but just incase...)
                synchronized (top_journal_lock) {
                    // Close all the journals
                    int sz = journal_archives.size();
                    for (int i = 0; i < sz; ++i) {
                        JournalFile jf = (JournalFile) journal_archives.get(i);
                        jf.close();
                    }
                    // Close the top journal
                    topJournal().close();
                    // Scan for journals and make the changes.
                    rollForwardRecover();
                }
            }

        }
    }

    /**
     * Recovers any lost operations that are currently in the journal.  This
     * retries all logged entries.  This would typically be called before any
     * other IO operations.
     */
    void rollForwardRecover() throws IOException {
//    System.out.println("rollForwardRecover()");

        // The list of all journal files,
        ArrayList journal_files_list = new ArrayList();

        // Scan the journal path for any journal files.
        for (int i = 10; i < 74; ++i) {
            String journal_fn = getJournalFileName(i);
            File f = new File(journal_path, journal_fn);
            // If the journal exists, create a summary of the journal
            if (f.exists()) {
                if (read_only) {
                    throw new IOException(
                            "Journal file " + f + " exists for a read-only session.  " +
                                    "There may not be any pending journals for a read-only session.");
                }

                JournalFile jf = new JournalFile(f, read_only);
                // Open the journal file for recovery.  This will set various
                // information about the journal such as the last check point and the
                // id of the journal file.
                JournalSummary summary = jf.openForRecovery();
                // If the journal can be recovered from.
                if (summary.can_be_recovered) {
                    if (debug.isInterestedIn(Lvl.INFORMATION)) {
                        debug.write(Lvl.INFORMATION, this, "Journal " + jf +
                                " found - can be recovered.");
                    }
                    journal_files_list.add(summary);
                } else {
                    if (debug.isInterestedIn(Lvl.INFORMATION)) {
                        debug.write(Lvl.INFORMATION, this, "Journal " + jf +
                                " deleting - nothing to recover.");
                    }
                    // Otherwise close and delete it
                    jf.closeAndDelete();
                }
            }
        }

//    if (journal_files_list.size() == 0) {
//      System.out.println("Nothing to recover.");
//    }

        // Sort the journal file list from oldest to newest.  The oldest journals
        // are recovered first.
        Collections.sort(journal_files_list, journal_list_comparator);

        long last_journal_number = -1;

        // Persist the journals
        for (int i = 0; i < journal_files_list.size(); ++i) {
            JournalSummary summary = (JournalSummary) journal_files_list.get(i);

            // Check the resources for this summary
            ArrayList res_list = summary.resource_list;
            for (int n = 0; n < res_list.size(); ++n) {
                String resource_name = (String) res_list.get(n);
                // This puts the resource into the hash map.
                JournalledResource resource = createResource(resource_name);
            }

            // Assert that we are recovering the journals in the correct order
            JournalFile jf = summary.journal_file;
            if (jf.journal_number < last_journal_number) {
                throw new Error("Assertion failed, sort failed.");
            }
            last_journal_number = jf.journal_number;

            if (debug.isInterestedIn(Lvl.INFORMATION)) {
                debug.write(Lvl.INFORMATION, this, "Recovering: " + jf +
                        " (8 .. " + summary.last_checkpoint + ")");
            }

            jf.persist(8, summary.last_checkpoint);
            // Then close and delete.
            jf.closeAndDelete();

            // Check the resources for this summary and close them
            for (int n = 0; n < res_list.size(); ++n) {
                String resource_name = (String) res_list.get(n);
                AbstractResource resource =
                        (AbstractResource) createResource(resource_name);
                // When we finished, make sure the resource is closed again
                // Close the resource
                resource.persistClose();
                // Post recover notification
                resource.notifyPostRecover();
            }

        }

    }

    private Comparator journal_list_comparator = new Comparator() {

        public int compare(Object ob1, Object ob2) {
            JournalSummary js1 = (JournalSummary) ob1;
            JournalSummary js2 = (JournalSummary) ob2;

            long jn1 = js1.journal_file.getJournalNumber();
            long jn2 = js2.journal_file.getJournalNumber();

            if (jn1 > jn2) {
                return 1;
            } else if (jn1 < jn2) {
                return -1;
            } else {
                return 0;
            }
        }

    };


    /**
     * Creates a new top journal file.
     */
    private void newTopJournalFile() throws IOException {
//    // Move the old journal to the archive?
//    if (top_journal_file != null) {
//      journal_archives.add(top_journal_file);
//    }

        String journal_fn = getJournalFileName((int) ((journal_number & 63) + 10));
        ++journal_number;

        File f = new File(journal_path, journal_fn);
        if (f.exists()) {
            throw new IOException("Journal file already exists.");
        }

        top_journal_file = new JournalFile(f, read_only);
        top_journal_file.open(journal_number - 1);
    }


    /**
     * Returns the current top journal file.
     */
    private JournalFile topJournal() {
        synchronized (top_journal_lock) {
            return top_journal_file;
        }
    }


    /**
     * Creates a resource.
     */
    public JournalledResource createResource(String resource_name) {
        AbstractResource resource;
        synchronized (all_resources) {
            // Has this resource previously been open?
            resource = (AbstractResource) all_resources.get(resource_name);
            if (resource == null) {
                // No...
                // Create a unique id for this
                final long id = seq_id;
                ++seq_id;
                // Create the StoreDataAccessor for this resource.
                StoreDataAccessor accessor =
                        sda_factory.createStoreDataAccessor(resource_name);
                if (ENABLE_LOGGING) {
                    resource = new Resource(resource_name, id, accessor);
                } else {
                    resource = new NonLoggingResource(resource_name, id, accessor);
                }
                // Put this in the map.
                all_resources.put(resource_name, resource);
            }
        }

        // Return the resource
        return resource;
    }

    /**
     * Sets a check point in the log.  If 'flush_journals' is true then when the
     * method returns we are guarenteed that all the journals are flushed and the
     * data is absolutely current.  If 'flush_journals' is false then we can't
     * assume the journals will be empty when the method returns.
     */
    void setCheckPoint(boolean flush_journals) throws IOException {
        // No Logging
        if (!ENABLE_LOGGING) {
            return;
        }
        // Return if read-only
        if (read_only) {
            return;
        }

        boolean something_to_persist;

        synchronized (top_journal_lock) {
            JournalFile top_j = topJournal();

            // When the journal exceeds a threshold then we cycle the top journal
            if (flush_journals || top_j.size() > (256 * 1024)) {
                // Cycle to the next journal file
                newTopJournalFile();
                // Add this to the archives
                journal_archives.add(top_j);
            }
            something_to_persist = journal_archives.size() > 0;
            top_j.setCheckPoint();
        }

        if (something_to_persist) {
            // Notifies the background thread that there is something to persist.
            // This will block until there are at most 10 journal files open.
            journaling_thread.persistArchives(10);
        }

    }

    /**
     * Returns the Resource with the given name.
     */
    private AbstractResource getResource(String resource_name) {
        synchronized (all_resources) {
            return (AbstractResource) all_resources.get(resource_name);
        }
    }


    // ---------- Inner classes ----------

    /**
     * A JournalFile represents a file in which modification are logged out to
     * when changes are made.  A JournalFile contains instructions for rebuilding
     * a resource to a known stable state.
     */
    private final class JournalFile {

        /**
         * The File object of this journal in the file system.
         */
        private File file;

        /**
         * True if the journal file is read only.
         */
        private boolean read_only;

        /**
         * The StreamFile object for reading and writing entries to/from the
         * journal.
         */
        private StreamFile data;

        /**
         * A DataOutputStream object used to write entries to the journal file.
         */
        private DataOutputStream data_out;

        /**
         * Small buffer.
         */
        private byte[] buffer;

        /**
         * A map between a resource name and an id for this journal file.
         */
        private HashMap resource_id_map;

        /**
         * The sequence id for resources modified in this log.
         */
        private long cur_seq_id;

        /**
         * The journal number of this journal.
         */
        private long journal_number;

        /**
         * True when open.
         */
        private boolean is_open;

        /**
         * The number of threads currently looking at info in this journal.
         */
        private int reference_count;

        /**
         * Constructs the journal file.
         */
        public JournalFile(File file, boolean read_only) {
            this.file = file;
            this.read_only = read_only;
            this.is_open = false;
            buffer = new byte[36];
            resource_id_map = new HashMap();
            cur_seq_id = 0;
            reference_count = 1;
        }

        /**
         * Returns the size of the journal file in bytes.
         */
        long size() {
            return data.length();
        }

        /**
         * Returns the journal number assigned to this journal.
         */
        long getJournalNumber() {
            return journal_number;
        }


        /**
         * Opens the journal file.  If the journal file exists then an error is
         * generated.
         */
        void open(long journal_number) throws IOException {
            if (is_open) {
                throw new IOException("Journal file is already open.");
            }
            if (file.exists()) {
                throw new IOException("Journal file already exists.");
            }

            this.journal_number = journal_number;
            data = new StreamFile(file, read_only ? "r" : "rw");
            data_out = new DataOutputStream(
                    new BufferedOutputStream(data.getOutputStream()));
            data_out.writeLong(journal_number);
            is_open = true;
        }

        /**
         * Opens the journal for recovery.  This scans the journal and generates
         * some statistics about the journal file such as the last check point and
         * the journal number.  If the journal file doesn't exist then an error is
         * generated.
         */
        JournalSummary openForRecovery() throws IOException {
            if (is_open) {
                throw new IOException("Journal file is already open.");
            }
            if (!file.exists()) {
                throw new IOException("Journal file does not exists.");
            }

            // Open the random access file to this journal
            data = new StreamFile(file, read_only ? "r" : "rw");
//      data_out = new DataOutputStream(
//                           new BufferedOutputStream(data.getOutputStream()));

            is_open = true;

            // Create the summary object (by default, not recoverable).
            JournalSummary summary = new JournalSummary(this);

            long end_pointer = data.length();

            // If end_pointer < 8 then can't recovert this journal
            if (end_pointer < 8) {
                return summary;
            }

            // The input stream.
            final DataInputStream din = new DataInputStream(
                    new BufferedInputStream(data.getInputStream()));

            try {
                // Set the journal number for this
                this.journal_number = din.readLong();
                long position = 8;

                ArrayList checkpoint_res_list = new ArrayList();

                // Start scan
                while (true) {

                    // If we can't read 12 bytes ahead, return the summary
                    if (position + 12 > end_pointer) {
                        return summary;
                    }

                    long type = din.readLong();
                    int size = din.readInt();

//          System.out.println("Scan: " + type + " pos=" + position + " size=" + size);
                    position = position + size + 12;

                    boolean skip_body = true;

                    // If checkpoint reached then we are recoverable
                    if (type == 100) {
                        summary.last_checkpoint = position;
                        summary.can_be_recovered = true;

                        // Add the resources in this check point
                        summary.resource_list.addAll(checkpoint_res_list);
                        // And clear the temporary list.
                        checkpoint_res_list.clear();

                    }

                    // If end reached, or type is not understood then return
                    else if (position >= end_pointer ||
                            type < 1 || type > 7) {
                        return summary;
                    }

                    // If we are resource type, then load the resource
                    if (type == 2) {

                        // We don't skip body for this type, we read the content
                        skip_body = false;
                        long id = din.readLong();
                        int str_len = din.readInt();
                        StringBuffer str = new StringBuffer(str_len);
                        for (int i = 0; i < str_len; ++i) {
                            str.append(din.readChar());
                        }

                        String resource_name = new String(str);
                        checkpoint_res_list.add(resource_name);

                    }

                    if (skip_body) {
                        int to_skip = size;
                        while (to_skip > 0) {
                            to_skip -= din.skip(to_skip);
                        }
                    }

                }

            } finally {
                din.close();
            }

        }

        /**
         * Closes the journal file.
         */
        void close() throws IOException {
            synchronized (this) {
                if (!is_open) {
                    throw new IOException("Journal file is already closed.");
                }

                data.close();
                data = null;
                is_open = false;
            }
        }

        /**
         * Returns true if the journal is deleted.
         */
        boolean isDeleted() {
            synchronized (this) {
                return data == null;
            }
        }

        /**
         * Closes and deletes the journal file.  This may not immediately close and
         * delete the journal file if there are currently references to it (for
         * example, in the middle of a read operation).
         */
        void closeAndDelete() throws IOException {
            synchronized (this) {
                --reference_count;
                if (reference_count == 0) {
                    // Close and delete the journal file.
                    close();
                    boolean b = file.delete();
                    if (!b) {
                        System.out.println("Unable to delete journal file: " + file);
                    }
                }
            }
        }

        /**
         * Adds a reference preventing the journal file from being deleted.
         */
        void addReference() {
            synchronized (this) {
                if (reference_count != 0) {
                    ++reference_count;
                }
            }
        }

        /**
         * Removes a reference, if we are at the last reference the journal file is
         * deleted.
         */
        void removeReference() throws IOException {
            closeAndDelete();
        }


        /**
         * Plays the log from the given offset in the file to the next checkpoint.
         * This will actually persist the log.  Returns -1 if the end of the journal
         * is reached.
         * <p>
         * NOTE: This will not verify that the journal is correct.  Verification
         *   should be done before the persist.
         */
        void persist(final long start, final long end) throws IOException {

            if (debug.isInterestedIn(Lvl.INFORMATION)) {
                debug.write(Lvl.INFORMATION, this, "Persisting: " + file);
            }

            final DataInputStream din = new DataInputStream(
                    new BufferedInputStream(data.getInputStream()));
            long count = start;
            // Skip to the offset
            while (count > 0) {
                count -= din.skip(count);
            }

            // The list of resources we updated
            ArrayList resources_updated = new ArrayList();

            // A map from resource id to resource name for this journal.
            HashMap id_name_map = new HashMap();

            boolean finished = false;
            long position = start;

            while (!finished) {
                long type = din.readLong();
                int size = din.readInt();
                position = position + size + 12;

                if (type == 2) {       // Resource id tag
                    long id = din.readLong();
                    int len = din.readInt();
                    StringBuffer buf = new StringBuffer(len);
                    for (int i = 0; i < len; ++i) {
                        buf.append(din.readChar());
                    }
                    String resource_name = new String(buf);

                    // Put this in the map
                    id_name_map.put(new Long(id), resource_name);

                    if (debug.isInterestedIn(Lvl.INFORMATION)) {
                        debug.write(Lvl.INFORMATION, this, "Journal Command: Tag: " + id +
                                " = " + resource_name);
                    }

                    // Add this to the list of resources we updated.
                    resources_updated.add(getResource(resource_name));

                } else if (type == 6) {  // Resource delete
                    long id = din.readLong();
                    String resource_name = (String) id_name_map.get(new Long(id));
                    AbstractResource resource = getResource(resource_name);

                    if (debug.isInterestedIn(Lvl.INFORMATION)) {
                        debug.write(Lvl.INFORMATION, this, "Journal Command: Delete: " +
                                resource_name);
                    }

                    resource.persistDelete();

                } else if (type == 3) {  // Resource size change
                    long id = din.readLong();
                    long new_size = din.readLong();
                    String resource_name = (String) id_name_map.get(new Long(id));
                    AbstractResource resource = getResource(resource_name);

                    if (debug.isInterestedIn(Lvl.INFORMATION)) {
                        debug.write(Lvl.INFORMATION, this, "Journal Command: Set Size: " +
                                resource_name + " size = " + new_size);
                    }

                    resource.persistSetSize(new_size);

                } else if (type == 1) {   // Page modification
                    long id = din.readLong();
                    long page = din.readLong();
                    int off = din.readInt();
                    int len = din.readInt();

                    String resource_name = (String) id_name_map.get(new Long(id));
                    AbstractResource resource = getResource(resource_name);

                    if (debug.isInterestedIn(Lvl.INFORMATION)) {
                        debug.write(Lvl.INFORMATION, this,
                                "Journal Command: Page Modify: " + resource_name +
                                        " page = " + page + " off = " + off +
                                        " len = " + len);
                    }

                    resource.persistPageChange(page, off, len, din);

                } else if (type == 100) { // Checkpoint (end)

                    if (debug.isInterestedIn(Lvl.INFORMATION)) {
                        debug.write(Lvl.INFORMATION, this, "Journal Command: Check Point.");
                    }

                    if (position == end) {
                        finished = true;
                    }
                } else {
                    throw new Error("Unknown tag type: " + type + " position = " + position);
                }

            }  // while (!finished)

            // Synch all the resources that we have updated.
            int sz = resources_updated.size();
            for (int i = 0; i < sz; ++i) {
                AbstractResource r = (AbstractResource) resources_updated.get(i);
                if (debug.isInterestedIn(Lvl.INFORMATION)) {
                    debug.write(Lvl.INFORMATION, this, "Synch: " + r);
                }
                r.synch();
            }

            din.close();

        }

        /**
         * Writes a resource identifier to the stream for the resource with the
         * given name.
         */
        private Long writeResourceName(String resource_name,
                                       DataOutputStream out) throws IOException {
            Long v;
            synchronized (resource_id_map) {
                v = (Long) resource_id_map.get(resource_name);
                if (v == null) {
                    ++cur_seq_id;

                    int len = resource_name.length();

                    // Write the header for this resource
                    out.writeLong(2);
                    out.writeInt(8 + 4 + (len * 2));
                    out.writeLong(cur_seq_id);
                    out.writeInt(len);
                    out.writeChars(resource_name);

                    // Put this id in the cache
                    v = new Long(cur_seq_id);
                    resource_id_map.put(resource_name, v);
                }
            }

            return v;
        }

        /**
         * Logs that a resource was deleted.
         */
        void logResourceDelete(String resource_name) throws IOException {

            synchronized (this) {
                // Build the header,
                Long v = writeResourceName(resource_name, data_out);

                // Write the header
                long resource_id = v.longValue();
                data_out.writeLong(6);
                data_out.writeInt(8);
                data_out.writeLong(resource_id);

            }

        }

        /**
         * Logs a resource size change.
         */
        void logResourceSizeChange(String resource_name, long new_size)
                throws IOException {
            synchronized (this) {
                // Build the header,
                Long v = writeResourceName(resource_name, data_out);

                // Write the header
                long resource_id = v.longValue();
                data_out.writeLong(3);
                data_out.writeInt(8 + 8);
                data_out.writeLong(resource_id);
                data_out.writeLong(new_size);

            }

        }

        /**
         * Sets a check point.  This will add an entry to the log.
         */
        void setCheckPoint() throws IOException {
            synchronized (this) {

                data_out.writeLong(100);
                data_out.writeInt(0);

                // Flush and synch the journal file
                flushAndSynch();
            }
        }

        /**
         * Logs a page modification to the end of the log and returns a pointer
         * in the file to the modification.
         */
        JournalEntry logPageModification(String resource_name, long page_number,
                                         byte[] buf, int off, int len) throws IOException {

            long ref;
            synchronized (this) {
                // Build the header,
                Long v = writeResourceName(resource_name, data_out);

                // The absolute position of the page,
                final long absolute_position = page_number * page_size;

                // Write the header
                long resource_id = v.longValue();
                data_out.writeLong(1);
                data_out.writeInt(8 + 8 + 4 + 4 + len);
                data_out.writeLong(resource_id);
//        data_out.writeLong(page_number);
//        data_out.writeInt(off);
                data_out.writeLong(absolute_position / 8192);
                data_out.writeInt(off + (int) (absolute_position & 8191));
                data_out.writeInt(len);

                data_out.write(buf, off, len);

                // Flush the changes so we can work out the pointer.
                data_out.flush();
                ref = data.length() - len - 36;
            }

            // Returns a JournalEntry object
            return new JournalEntry(resource_name, this, ref, page_number);
        }


        /**
         * Reconstructs a modification that is logged in this journal.
         */
        void buildPage(long in_page_number,
                       long position, byte[] buf, int off) throws IOException {
            long type;
            long resource_id;
            long page_number;
            int page_offset;
            int page_length;

            synchronized (this) {
                data.readFully(position, buffer, 0, 36);
                type = ByteArrayUtil.getLong(buffer, 0);
                resource_id = ByteArrayUtil.getLong(buffer, 12);
                page_number = ByteArrayUtil.getLong(buffer, 20);
                page_offset = ByteArrayUtil.getInt(buffer, 28);
                page_length = ByteArrayUtil.getInt(buffer, 32);

                // Some asserts,
                if (type != 1) {
                    throw new IOException("Invalid page type. type = " + type +
                            " pos = " + position);
                }
                if (page_number != in_page_number) {
                    throw new IOException("Page numbers do not match.");
                }

                // Read the content.
                data.readFully(position + 36, buf, off + page_offset, page_length);
            }

        }

        /**
         * Synchronizes the log.
         */
        void flushAndSynch() throws IOException {
            synchronized (this) {
                data_out.flush();
                data.synch();
            }
        }


        public String toString() {
            return "[JOURNAL: " + file.getName() + "]";
        }

    }

    /**
     * A JournalEntry represents a modification that has been logging in the
     * journal for a specific page of a resource.  It contains the name of the
     * log file, the position in the journal of the modification, and the page
     * number.
     */
    private static final class JournalEntry {

        /**
         * The resource that this page is on.
         */
        private final String resource_name;

        /**
         * The journal file.
         */
        private final JournalFile journal;

        /**
         * The position in the journal file.
         */
        private final long position;

        /**
         * The page number of this modification.
         */
        private final long page_number;


        /**
         * The next journal entry with the same page number
         */
        JournalEntry next_page;


        /**
         * Constructs the entry.
         */
        public JournalEntry(String resource_name, JournalFile journal,
                            long position, long page_number) {
            this.resource_name = resource_name;
            this.journal = journal;
            this.position = position;
            this.page_number = page_number;
        }

        /**
         * Returns the journal file for this entry.
         */
        public JournalFile getJournalFile() {
            return journal;
        }

        /**
         * Returns the position of the log entry in the journal file.
         */
        public long getPosition() {
            return position;
        }

        /**
         * Returns the page number of this modification log entry.
         */
        public long getPageNumber() {
            return page_number;
        }

    }


    /**
     * An abstract resource.
     */
    private abstract class AbstractResource implements JournalledResource {

        /**
         * The unique name given this resource (the file name).
         */
        protected final String name;

        /**
         * The id assigned to this resource by this session.  This id should not
         * be used in any external source.
         */
        protected final long id;

        /**
         * The backing object.
         */
        protected final StoreDataAccessor data;

        /**
         * True if this resource is read_only.
         */
        protected boolean read_only;

        /**
         * Constructs the resource.
         */
        AbstractResource(String name, long id, StoreDataAccessor data) {
            this.name = name;
            this.id = id;
            this.data = data;
        }


        // ---------- Persist methods ----------

        abstract void persistClose() throws IOException;

        abstract void persistDelete() throws IOException;

        abstract void persistSetSize(final long new_size) throws IOException;

        abstract void persistPageChange(final long page,
                                        final int off, int len,
                                        DataInputStream din) throws IOException;

        abstract void synch() throws IOException;

        // Called after a rollForwardRecover to notify the resource to update its
        // state to reflect the fact that changes have occurred.
        abstract void notifyPostRecover();

        // ----------

        /**
         * Returns the size of the page.
         */
        public int getPageSize() {
            return page_size;
        }

        /**
         * Returns the unique id of this page.
         */
        public long getID() {
            return id;
        }


        public String toString() {
            return name;
        }

    }

    /**
     * An implementation of AbstractResource that doesn't log.
     */
    private final class NonLoggingResource extends AbstractResource {

        /**
         * Constructs the resource.
         */
        NonLoggingResource(String name, long id, StoreDataAccessor data) {
            super(name, id, data);
        }


        // ---------- Persist methods ----------

        void persistClose() throws IOException {
            // No-op
        }

        public void persistDelete() throws IOException {
            // No-op
        }

        public void persistSetSize(final long new_size) throws IOException {
            // No-op
        }

        public void persistPageChange(final long page,
                                      final int off, int len,
                                      DataInputStream din) throws IOException {
            // No-op
        }

        public void synch() throws IOException {
            data.synch();
        }

        public void notifyPostRecover() {
            // No-op
        }

        // ----------

        /**
         * Opens the resource.
         */
        public void open(boolean read_only) throws IOException {
            this.read_only = read_only;
            data.open(read_only);
        }

        /**
         * Reads a page from the resource.
         */
        public void read(final long page_number,
                         final byte[] buf, final int off) throws IOException {
            // Read the data.
            long page_position = page_number * page_size;
            data.read(page_position + off, buf, off, page_size);
        }

        /**
         * Writes a page of some previously specified size.
         */
        public void write(final long page_number,
                          byte[] buf, int off, int len) throws IOException {
            long page_position = page_number * page_size;
            data.write(page_position + off, buf, off, len);
        }

        /**
         * Sets the size of the resource.
         */
        public void setSize(long size) throws IOException {
            data.setSize(size);
        }

        /**
         * Returns the size of this resource.
         */
        public long getSize() throws IOException {
            return data.getSize();
        }

        /**
         * Closes the resource.
         */
        public void close() throws IOException {
            data.close();
        }

        /**
         * Deletes the resource.
         */
        public void delete() throws IOException {
            data.delete();
        }

        /**
         * Returns true if the resource currently exists.
         */
        public boolean exists() {
            return data.exists();
        }

    }

    /**
     * Represents a resource in this system.  A resource is backed by a
     * StoreDataAccessor and may have one or more modifications to it in the
     * journal.
     */
    private final class Resource extends AbstractResource {

        /**
         * The size of the resource.
         */
        private long size;

        /**
         * True if there is actually data to be read in the above object.
         */
        private boolean there_is_backing_data;

        /**
         * True if the underlying resource is really open.
         */
        private boolean really_open;

        /**
         * True if the data store exists.
         */
        private boolean data_exists;

        /**
         * True if the data resource is open.
         */
        private boolean data_open;

        /**
         * True if the data resource was deleted.
         */
        private boolean data_deleted;

        /**
         * The hash of all journal entries on this resource (JournalEntry).
         */
        private final JournalEntry[] journal_map;

        /**
         * A temporary buffer the size of a page.
         */
        private final byte[] page_buffer;

        /**
         * Constructs the resource.
         */
        Resource(String name, long id, StoreDataAccessor data) {
            super(name, id, data);
            journal_map = new JournalEntry[257];
            data_open = false;
            data_exists = data.exists();
            data_deleted = false;
            if (data_exists) {
                try {
                    size = data.getSize();
//          System.out.println("Setting size of " + name + " to " + size);
                } catch (IOException e) {
                    throw new Error("Error getting size of resource: " + e.getMessage());
                }
            }
            really_open = false;
            page_buffer = new byte[page_size];
        }


        // ---------- Persist methods ----------

        private void persistOpen(boolean read_only) throws IOException {
//      System.out.println(name + " Open");
            if (!really_open) {
                data.open(read_only);
                there_is_backing_data = true;
                really_open = true;
            }
        }

        void persistClose() throws IOException {
//      System.out.println(name + " Close");
            if (really_open) {
                // When we close we reset the size attribute.  We do this because of
                // the roll forward recovery.
                size = data.getSize();
                data.synch();
                data.close();
                really_open = false;
            }
        }

        public void persistDelete() throws IOException {
//      System.out.println(name + " Delete");
            // If open then close
            if (really_open) {
                persistClose();
            }
            data.delete();
            there_is_backing_data = false;
        }

        public void persistSetSize(final long new_size) throws IOException {
//      System.out.println(name + " Set Size " + size);
            // If not open then open.
            if (!really_open) {
                persistOpen(false);
            }
            // Don't let us set a size that's smaller than the current size.
            if (new_size > data.getSize()) {
                data.setSize(new_size);
            }
        }

        public void persistPageChange(final long page,
                                      final int off, int len,
                                      DataInputStream din) throws IOException {
            if (!really_open) {
                persistOpen(false);
            }

            // Buffer to read the page content into
            byte[] buf;
            if (len <= page_buffer.length) {
                // If length is smaller or equal to the size of a page then use the
                // local page buffer.
                buf = page_buffer;
            } else {
                // Otherwise create a new buffer of the required size (this may happen
                // if the page size changes between sessions).
                buf = new byte[len];
            }

            // Read the change from the input stream
            din.readFully(buf, 0, len);
            // Write the change out to the underlying resource container
            long pos = page * 8192; //page_size;
            data.write(pos + off, buf, 0, len);
        }

        public void synch() throws IOException {
            if (really_open) {
                data.synch();
            }
        }

        public void notifyPostRecover() {
            data_exists = data.exists();
        }


        // ----------

        /**
         * Opens the resource.  This method will check if the resource exists.  If
         * it doesn't exist the 'read' method will return just the journal
         * modifications of a page.  If it does exist it opens the resource and uses
         * that as the backing to any 'read' operations.
         */
        public void open(boolean read_only) throws IOException {
            this.read_only = read_only;

            if (!data_deleted && data.exists()) {
                // It does exist so open it.
                persistOpen(read_only);
            } else {
                there_is_backing_data = false;
                data_deleted = false;
            }
            data_open = true;
            data_exists = true;
        }

        /**
         * Reads a page from the resource.  This method reconstructs the page
         * from the underlying data, and from any journal entries.  This should
         * read the data to be put into a buffer in memory.
         */
        public void read(final long page_number,
                         final byte[] buf, final int off) throws IOException {

            synchronized (journal_map) {
                if (!data_open) {
                    throw new IOException("Assertion failed: Data file is not open.");
                }
            }

            // The list of all journal entries on this page number
            final ArrayList all_journal_entries = new ArrayList(4);
            try {
                // The map index.
                synchronized (journal_map) {
                    int i = ((int) (page_number & 0x0FFFFFFF) % journal_map.length);
                    JournalEntry entry = (JournalEntry) journal_map[i];
                    JournalEntry prev = null;

                    while (entry != null) {
                        boolean deleted_hash = false;

                        JournalFile file = entry.getJournalFile();
                        // Note that once we have a reference the journal file can not be
                        // deleted.
                        file.addReference();

                        // If the file is closed (or deleted)
                        if (file.isDeleted()) {
                            deleted_hash = true;
                            // Deleted so remove the reference to the journal
                            file.removeReference();
                            // Remove the journal entry from the chain.
                            if (prev == null) {
                                journal_map[i] = entry.next_page;
                            } else {
                                prev.next_page = entry.next_page;
                            }
                        }
                        // Else if not closed then is this entry the page number?
                        else if (entry.getPageNumber() == page_number) {
                            all_journal_entries.add(entry);
                        } else {
                            // Not the page we are looking for so remove the reference to the
                            // file.
                            file.removeReference();
                        }

                        // Only move prev is we have NOT deleted a hash entry
                        if (!deleted_hash) {
                            prev = entry;
                        }
                        entry = entry.next_page;
                    }
                }

                // Read any data from the underlying file
                if (there_is_backing_data) {
                    long page_position = page_number * page_size;
                    // First read the page from the underlying store.
                    data.read(page_position, buf, off, page_size);
                } else {
                    // Clear the buffer
                    for (int i = off; i < (page_size + off); ++i) {
                        buf[i] = 0;
                    }
                }

                // Rebuild from the journal file(s)
                final int sz = all_journal_entries.size();
                for (int i = 0; i < sz; ++i) {
                    JournalEntry entry = (JournalEntry) all_journal_entries.get(i);
                    JournalFile file = entry.getJournalFile();
                    final long position = entry.getPosition();
                    synchronized (file) {
                        file.buildPage(page_number, position, buf, off);
                    }
                }

            } finally {

                // Make sure we remove the reference for all the journal files.
                final int sz = all_journal_entries.size();
                for (int i = 0; i < sz; ++i) {
                    JournalEntry entry = (JournalEntry) all_journal_entries.get(i);
                    JournalFile file = entry.getJournalFile();
                    file.removeReference();
                }

            }

        }

        /**
         * Writes a page of some previously specified size to the top log.  This
         * will add a single entry to the log and any 'read' operations after will
         * contain the written data.
         */
        public void write(final long page_number,
                          byte[] buf, int off, int len) throws IOException {

            synchronized (journal_map) {
                if (!data_open) {
                    throw new IOException("Assertion failed: Data file is not open.");
                }

                // Make this modification in the log
                JournalEntry journal;
                synchronized (top_journal_lock) {
                    journal = topJournal().logPageModification(name, page_number,
                            buf, off, len);
                }

                // This adds the modification to the END of the hash list.  This means
                // when we reconstruct the page the journals will always be in the
                // correct order - from oldest to newest.

                // The map index.
                int i = ((int) (page_number & 0x0FFFFFFF) % journal_map.length);
                JournalEntry entry = (JournalEntry) journal_map[i];
                // Make sure this entry is added to the END
                if (entry == null) {
                    // Add at the head if no first entry
                    journal_map[i] = journal;
                    journal.next_page = null;
                } else {
                    // Otherwise search to the end
                    // The number of journal entries in the linked list
                    int journal_entry_count = 0;
                    while (entry.next_page != null) {
                        entry = entry.next_page;
                        ++journal_entry_count;
                    }
                    // and add to the end
                    entry.next_page = journal;
                    journal.next_page = null;

                    // If there are over 35 journal entries, scan and remove all entries
                    // on journals that have persisted
                    if (journal_entry_count > 35) {
                        int entries_cleaned = 0;
                        entry = (JournalEntry) journal_map[i];
                        JournalEntry prev = null;

                        while (entry != null) {
                            boolean deleted_hash = false;

                            JournalFile file = entry.getJournalFile();
                            // Note that once we have a reference the journal file can not be
                            // deleted.
                            file.addReference();

                            // If the file is closed (or deleted)
                            if (file.isDeleted()) {
                                deleted_hash = true;
                                // Deleted so remove the reference to the journal
                                file.removeReference();
                                // Remove the journal entry from the chain.
                                if (prev == null) {
                                    journal_map[i] = entry.next_page;
                                } else {
                                    prev.next_page = entry.next_page;
                                }
                                ++entries_cleaned;
                            }
                            // Remove the reference
                            file.removeReference();

                            // Only move prev is we have NOT deleted a hash entry
                            if (!deleted_hash) {
                                prev = entry;
                            }
                            entry = entry.next_page;
                        }

                    }
                }
            }

        }

        /**
         * Sets the size of the resource.
         */
        public void setSize(long size) throws IOException {
            synchronized (journal_map) {
                this.size = size;
            }
            synchronized (top_journal_lock) {
                topJournal().logResourceSizeChange(name, size);
            }
        }

        /**
         * Returns the size of this resource.
         */
        public long getSize() throws IOException {
            synchronized (journal_map) {
                return this.size;
            }
        }

        /**
         * Closes the resource.  This will actually simply log that the resource
         * has been closed.
         */
        public void close() throws IOException {
            synchronized (journal_map) {
                data_open = false;
            }
        }

        /**
         * Deletes the resource.  This will actually simply log that the resource
         * has been deleted.
         */
        public void delete() throws IOException {
            // Log that this resource was deleted.
            synchronized (top_journal_lock) {
                topJournal().logResourceDelete(name);
            }
            synchronized (journal_map) {
                data_exists = false;
                data_deleted = true;
                size = 0;
            }
        }

        /**
         * Returns true if the resource currently exists.
         */
        public boolean exists() {
            return data_exists;
        }

    }

    /**
     * Summary information about a journal.
     */
    private static class JournalSummary {

        /**
         * The JournalFile object that is a summary of.
         */
        JournalFile journal_file;

        /**
         * True if the journal is recoverable (has one or more complete check
         * points available).
         */
        boolean can_be_recovered = false;

        /**
         * The position of the last checkpoint in the journal.
         */
        long last_checkpoint;

        /**
         * The list of all resource names that this journal 'touches'.
         */
        ArrayList resource_list = new ArrayList();

        /**
         * Constructor.
         */
        public JournalSummary(JournalFile journal_file) {
            this.journal_file = journal_file;
        }

    }

    /**
     * Thread that persists the journal in the backgroudn.
     */
    private class JournalingThread extends Thread {

        private boolean finished = false;
        private boolean actually_finished;

        /**
         * Constructor.
         */
        JournalingThread() {
            setName("Pony - Background Journaling");
            // This is a daemon thread.  it should be safe if this thread
            // dies at any time.
            setDaemon(true);
        }


        public void run() {
            boolean local_finished = false;

            while (!local_finished) {

                ArrayList to_process = null;
                synchronized (top_journal_lock) {
                    if (journal_archives.size() > 0) {
                        to_process = new ArrayList();
                        to_process.addAll(journal_archives);
                    }
                }

                if (to_process == null) {
                    // Nothing to process so wait
                    synchronized (this) {
                        if (!finished) {
                            try {
                                wait();
                            } catch (InterruptedException e) { /* ignore */ }
                        }
                    }

                } else if (to_process.size() > 0) {
                    // Something to process, so go ahead and process the journals,
                    int sz = to_process.size();
                    // For all journals
                    for (int i = 0; i < sz; ++i) {
                        // Pick the lowest journal to persist
                        JournalFile jf = (JournalFile) to_process.get(i);
                        try {
                            // Persist the journal
                            jf.persist(8, jf.size());
                            // Close and then delete the journal file
                            jf.closeAndDelete();
                        } catch (IOException e) {
                            debug.write(Lvl.ERROR, this, "Error persisting journal: " + jf);
                            debug.writeException(Lvl.ERROR, e);
                            // If there is an error persisting the best thing to do is
                            // finish
                            synchronized (this) {
                                finished = true;
                            }
                        }
                    }
                }

                synchronized (this) {
                    local_finished = finished;
                    // Remove the journals that we have just persisted.
                    if (to_process != null) {
                        synchronized (top_journal_lock) {
                            int sz = to_process.size();
                            for (int i = 0; i < sz; ++i) {
                                journal_archives.remove(0);
                            }
                        }
                    }
                    // Notify any threads waiting
                    notifyAll();
                }

            }

            synchronized (this) {
                actually_finished = true;
                notifyAll();
            }
        }

        public synchronized void finish() {
            finished = true;
            notifyAll();
        }

        public synchronized void waitUntilFinished() {
            try {
                while (!actually_finished) {
                    wait();
                }
            } catch (InterruptedException e) {
                throw new Error("Interrupted: " + e.getMessage());
            }
        }

        /**
         * Persists the journal_archives list until the list is at least the
         * given size.
         */
        public synchronized void persistArchives(int until_size) {
            notifyAll();
            int sz;
            synchronized (top_journal_lock) {
                sz = journal_archives.size();
            }
            // Wait until the sz is smaller than 'until_size'
            while (sz > until_size) {
                try {
                    wait();
                } catch (InterruptedException e) { /* ignore */ }

                synchronized (top_journal_lock) {
                    sz = journal_archives.size();
                }
            }
        }

    }

}

