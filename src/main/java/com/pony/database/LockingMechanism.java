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

import java.util.HashMap;

/**
 * This class represents a model for locking the tables in a database during
 * any sequence of concurrent read/write accesses.
 * <p>
 * Every table in the database has an 'access_queue' that is generated the
 * first time the table is accessed.  When a read or write request happens,
 * the thread and the type of access is put onto the top of the queue.  When
 * the read/write access to the table has completed, the access is removed
 * from the queue.
 * <p>
 * An access to the table may be 'blocked' until other threads have completed
 * their access of the table.
 * <p>
 * A table that has a 'read lock' can not be altered until the table object
 * is released.  A table that has a 'write lock' may not be read until the
 * table object is released.
 * <p>
 * The general rules are:
 *   a) A read request can go ahead if there are no write request infront of
 *      this request in the access queue.
 *   b) A write request can go ahead if the write request is at the front of
 *      the access queue.
 * <p>
 * This class requires some for-sight to which tables will be read/written
 * to.  We must pass all tables being read/written in a single stage.  This
 * implies a 2 stage process, the 1st determining which tables are being
 * accessed and the 2nd performing the actual operations.
 * <p>
 * Some operations such as creating and dropping and modifying the security
 * tables may require that no threads interfere with the database state while
 * the operation is occuring.  This is handled through an 'Excluside Mode'.
 * When an object calls the locking mechanism to switch into exclusive mode, it
 * blocks until all access to the database are complete, then continues,
 * blocking all other threads until the exclusive mode is cancelled.
 * <p>
 * The locking system, in simple terms, ensures that any multiple read
 * operations will happen concurrently, however write operations block until
 * all operations are complete.
 * <p>
 * SYNCHRONIZATION: This method implements some important concurrent models
 *   for ensuring that queries can never be corrupted.
 * <p>
 * @author Tobias Downer
 */

public final class LockingMechanism {

    /**
     * Class statics.  These are used in the 'setMode' method to request either
     * shared or exclusive access to the database.
     */
    public final static int SHARED_MODE = 1;
    public final static int EXCLUSIVE_MODE = 2;

    /**
     * This Hashtable is a mapping from a 'DataTable' to the 'LockingQueue'
     * object that is available for it.
     */
    private HashMap queues_map = new HashMap();

    /**
     * This boolean is set as soon as a Thread requests to go into 'exclusive
     * mode'.
     */
    private boolean in_exclusive_mode = false;

    /**
     * This contains the number of Threads that have requested to go into
     * 'shared mode'.  It is incremented each time 'setMode(SHARED_MODE)' is
     * called.
     */
    private int shared_mode = 0;

    /**
     * The DebugLogger object that we log debug messages to.
     */
    private final DebugLogger debug;

    /**
     * Constructor.
     */
    public LockingMechanism(DebugLogger logger) {
        this.debug = logger;
    }

    /**
     * This is a helper function for returning the LockingQueue object for the
     * DataTable object.  If there has not previously been a queue instantiated
     * for the table, it creates a new one and adds it to the Hashtable.
     * <p>
     * ISSUE: Not synchronized because we guarenteed to be called from a
     *   synchronized method right?
     */
    private LockingQueue getQueueFor(DataTable table) {
        LockingQueue queue = (LockingQueue) queues_map.get(table);

        // If queue not in hashtable then create a new one and put it into mapping
        if (queue == null) {
            queue = new LockingQueue(table);
            queues_map.put(table, queue);
        }

        return queue;
    }

    /**
     * Resets this object so it may be reused.  This will release all internal
     * DataTable queues that are being kept.
     */
    public void reset() {

        synchronized (this) {
            // Check we are in exclusive mode,
            if (!isInExclusiveMode()) {
                // This is currently just a warning but should be upgraded to a
                // full error.
                debug.writeException(new RuntimeException("Should not clear a " +
                        "LockingMechanism that's not in exclusive mode."));
            }
            queues_map.clear();
        }

    }

    /**
     * This method locks the given tables for either reading or writing.  It
     * puts the access locks in a queue for the given tables.  This 'reserves'
     * the rights for this thread to access the table in that way.  This
     * reservation can be used by the system to decide table accessability.
     * <p>
     * NOTE: ** IMPORTANT ** We must ensure that a single Thread can not create
     *   multiple table locks.  Otherwise it will cause situations where deadlock
     *   can result.
     * NOTE: ** IMPORTANT ** We must ensure that once a lock has occured, it
     *   is unlocked at a later time _no matter what happens_.  Otherwise there
     *   will be situations where deadlock can result.
     * NOTE: A LockHandle should not be given to another Thread.
     * <p>
     * SYNCHRONIZATION: This method is synchronized to ensure multiple additions
     *   to the locking queues can happen without interference.
     */
    public LockHandle lockTables(DataTable[] t_write, DataTable[] t_read) {

        // Set up the local constants.

        final int lock_count = t_read.length + t_write.length;
        final LockHandle handle = new LockHandle(lock_count, debug);

        synchronized (this) {

            Lock lock;
            LockingQueue queue;
            int queue_index;

            // Add read and write locks to cache and to the handle.

            for (int i = t_write.length - 1; i >= 0; --i) {
                DataTable to_write_lock = t_write[i];
                queue = getQueueFor(to_write_lock);
                // slightly confusing: this will add lock to given table queue
                lock = new Lock(Lock.WRITE, queue, debug);
                handle.addLock(lock);

                debug.write(Lvl.INFORMATION, this,
                        "[LockingMechanism] Locking for WRITE: " +
                                to_write_lock.getTableName());
            }

            for (int i = t_read.length - 1; i >= 0; --i) {
                DataTable to_read_lock = t_read[i];
                queue = getQueueFor(to_read_lock);
                // slightly confusing: this will add lock to given table queue
                lock = new Lock(Lock.READ, queue, debug);
                handle.addLock(lock);

                debug.write(Lvl.INFORMATION, this,
                        "[LockingMechanism] Locking for READ: " +
                                to_read_lock.getTableName());
            }

        }

        debug.write(Lvl.INFORMATION, this, "Locked Tables");

        return handle;

    }

    /**
     * Unlocks the tables that were previously locked by the 'lockTables' method.
     * It is required that this method is called after the table references made
     * by a query are released (set to null or forgotten). This usually means
     * _after_ the result set has been written to the client.
     * SYNCHRONIZATION: This method is synchronized so concurrent unlocking
     *   can not corrupt the queues.
     */
    public void unlockTables(LockHandle handle) {
        synchronized (this) {
            handle.unlockAll();
        }
        debug.write(Lvl.INFORMATION, this, "UnLocked Tables");
    }

    /**
     * Returns true if we are locked into exclusive mode.
     */
    public synchronized boolean isInExclusiveMode() {
        return in_exclusive_mode;
    }

    /**
     * This method _must_ be called before a threads initial access to a Database
     * object.  It registers whether the preceding database accesses will be in
     * an 'exclusive mode' or a 'shared mode'.  In shared mode, any number of
     * threads are able to access the database.  In exclusive, the current thread
     * may be the only one that may access the database.
     * On requesting exclusive mode, it blocks until exclusive mode is available.
     * On requesting shared mode, it blocks only if currently in exclusive mode.
     * NOTE: 'exclusive mode' should be used only in system maintenance type
     *   operations such as creating and dropping tables from the database.
     */
    public synchronized void setMode(int mode) {

        // If currently in exclusive mode, block until not.

        while (in_exclusive_mode == true) {
            try {
//        System.out.println("Waiting because in exclusive lock.");
                wait();
//        System.out.println("Finish: Waiting because in exclusive lock.");
            } catch (InterruptedException e) {
            }
        }

        if (mode == EXCLUSIVE_MODE) {

            // Set this thread to exclusive mode, and wait until all shared modes
            // have completed.

            in_exclusive_mode = true;
            while (shared_mode > 0) {
                try {
//          System.out.println("Waiting on exclusive lock: " + shared_mode);
                    wait();
//          System.out.println("Finish: Waiting on exclusive lock: " + shared_mode);
                } catch (InterruptedException e) {
                }
            }

            debug.write(Lvl.INFORMATION, this, "Locked into ** EXCLUSIVE MODE **");

        } else if (mode == SHARED_MODE) {

            // Increase the threads counter that are in shared mode.

            ++shared_mode;

            debug.write(Lvl.INFORMATION, this, "Locked into SHARED MODE");

        } else {
            throw new Error("Invalid mode");
        }
    }

    /**
     * This must be called when the calls to a Database object have finished.
     * It 'finishes' the mode that the locking mechanism was set into by the
     * call to the 'setMode' method.
     * NOTE: ** IMPORTANT ** This method __MUST__ be guarenteed to be called some
     *   time after the 'setMode' method.  Otherwise deadlock.
     */
    public synchronized void finishMode(int mode) {
        if (mode == EXCLUSIVE_MODE) {
            in_exclusive_mode = false;
            notifyAll();

            debug.write(Lvl.INFORMATION, this, "UnLocked from ** EXCLUSIVE MODE **");

        } else if (mode == SHARED_MODE) {
            --shared_mode;
            if (shared_mode == 0 && in_exclusive_mode) {
                notifyAll();
            } else if (shared_mode < 0) {
                shared_mode = 0;
                notifyAll();
                throw new RuntimeException("Too many 'finishMode(SHARED_MODE)' calls");
            }

            debug.write(Lvl.INFORMATION, this, "UnLocked from SHARED MODE");

        } else {
            throw new Error("Invalid mode");
        }
    }

}
