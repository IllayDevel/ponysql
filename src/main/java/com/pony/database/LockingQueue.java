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

package com.pony.database;

import com.pony.debug.*;

import java.util.ArrayList;

/**
 * This class is used in the 'LockingMechanism' class.  It maintains a queue
 * of threads that have locked the table this queue refers to.  A lock means
 * the table is either pending to be accessed, or the data in the table is
 * being used.
 * <p>
 * A write lock in the queue stops any concurrently running threads from
 * accessing the tables.  A read lock can go ahead only if there is no write
 * lock in the queue below it.
 * <p>
 * The rules are simple, and allow for reading of tables to happen concurrently
 * and writing to happen sequentually.  Once a table is pending being written
 * to, it must be guarenteed that no thread can read the table while the write
 * is happening.
 * <p>
 * @author Tobias Downer
 */

final class LockingQueue {

    /**
     * The DataTable this queue is 'protecting'
     */
    private final DataTable parent_table;

    /**
     * This is the queue that stores the table locks.
     */
    private final ArrayList queue;

    /**
     * The Constructor.
     */
    LockingQueue(DataTable table) {
        parent_table = table;
        queue = new ArrayList();
    }

    /**
     * Returns the DataTable object the queue is 'attached' to.
     */
    DataTable getTable() {
        return parent_table;
    }

    /**
     * Adds a lock to the queue.
     * NOTE: This method is thread safe since it is only called from the
     *   LockingMechanism synchronized methods.
     * SYNCHRONIZED: This has to be synchronized because we don't want new locks
     *   being added while a 'checkAccess' is happening.
     */
    synchronized void addLock(Lock lock) {
        queue.add(lock);
    }

    /**
     * Removes a lock from the queue.  This also does a 'notifyAll()' to kick any
     * threads that might be blocking in the 'checkAccess' method.
     * SYNCHRONIZED: This has to be synchronized because we don't want locks to
     *   be removed while a 'checkAccess' is happening.
     */
    synchronized void removeLock(Lock lock) {
        queue.remove(lock);
        // Notify the table that we have released a lock from it.
        lock.getTable().notifyReleaseRWLock(lock.getType());
//    System.out.println("Removing lock: " + lock);
        notifyAll();
    }

    /**
     * Looks at the queue and _blocks_ if the access to the table by the means
     * specified in the lock is allowed or not.
     * The rules for determining this are as follows:
     * <p>
     *   1) If the lock is a READ lock and there is a WRITE lock 'infront' of
     *      this lock on the queue then block.
     *   2) If the lock is a WRITE lock and the lock isn't at the front of the
     *      queue then block.
     *   3) Retry when a lock is released from the queue.
     */
    void checkAccess(Lock lock) {
        boolean blocked;
        int index, i;
        Lock test_lock;

        synchronized (this) {

            // Error checking.  The queue must contain the lock.
            if (!queue.contains(lock)) {
                throw new Error("Queue does not contain the given lock");
            }

            // If 'READ'
            if (lock.getType() == Lock.READ) {

                do {
                    blocked = false;

                    index = queue.indexOf(lock);
                    for (i = index - 1; i >= 0 && blocked == false; --i) {
                        test_lock = (Lock) queue.get(i);
                        if (test_lock.getType() == Lock.WRITE) {
                            blocked = true;
                        }
                    }

                    if (blocked == true) {
                        getTable().Debug().write(Lvl.INFORMATION, this,
                                "Blocking on read.");
//            System.out.println("READ BLOCK: " + queue);
                        try {
                            wait();
                        } catch (InterruptedException ignore) {
                        }
                    }

                } while (blocked == true);

            }

            // Else must be 'WRITE'
            else {

                do {
                    blocked = false;

                    index = queue.indexOf(lock);
                    if (index != 0) {
                        blocked = true;
                        getTable().Debug().write(Lvl.INFORMATION, this,
                                "Blocking on write.");
//            System.out.println("WRITE BLOCK: " + queue);
                        try {
                            wait();
                        } catch (InterruptedException ignore) {
                        }
                    }

                } while (blocked == true);

            }

            // Notify the Lock table that we've got a lock on it.
            lock.getTable().notifyAddRWLock(lock.getType());

        } /* synchronized (this) */

    }

    public synchronized String toString() {
        StringBuffer str = new StringBuffer("[LockingQueue]: (");
        for (Object o : queue) {
            str.append(o);
            str.append(", ");
        }
        str.append(")");
        return new String(str);
    }

}
