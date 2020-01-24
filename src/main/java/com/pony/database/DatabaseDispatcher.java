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

import java.util.*;

import com.pony.debug.*;

/**
 * This is the database system dispatcher thread.  This is a thread that
 * runs in the background servicing delayed events.  This thread serves a
 * number of purposes.  It can be used to perform optimizations/clean ups in
 * the background (similar to hotspot).  It could be used to pause until
 * sufficient information has been collected or there is a lull in
 * work before it does a query in the background.  For example, if a VIEW
 * is invalidated because the underlying data changes, then we can wait
 * until the data has finished updating, then perform the view query to
 * update it correctly.
 *
 * @author Tobias Downer
 */

class DatabaseDispatcher extends Thread {

    private final ArrayList event_queue = new ArrayList();

    private final TransactionSystem system;

    private boolean finished;

    /**
     * NOTE: Constructing this object will start the thread.
     */
    DatabaseDispatcher(TransactionSystem system) {
        this.system = system;
        setDaemon(true);
        setName("Pony - Database Dispatcher");
        finished = false;
        start();
    }

    /**
     * Creates an event object that is passed into 'addEventToDispatch' method
     * to run the given Runnable method after the time has passed.
     * <p>
     * The event created here can be safely posted on the event queue as many
     * times as you like.  It's useful to create an event as a persistant object
     * to service some event.  Just post it on the dispatcher when you want
     * it run!
     */
    Object createEvent(Runnable runnable) {
        return new DatabaseEvent(runnable);
    }

    /**
     * Adds a new event to be dispatched on the queue after 'time_to_wait'
     * milliseconds has passed.
     */
    synchronized void postEvent(int time_to_wait, Object event) {
        DatabaseEvent evt = (DatabaseEvent) event;
        // Remove this event from the queue,
        event_queue.remove(event);
        // Set the correct time for the event.
        evt.time_to_run_event = System.currentTimeMillis() + time_to_wait;
        // Add to the queue in correct order
        int index = Collections.binarySearch(event_queue, event);
        if (index < 0) {
            index = -(index + 1);
        }
        event_queue.add(index, event);

        notifyAll();
    }

    /**
     * Ends this dispatcher thread.
     */
    synchronized void finish() {
        finished = true;
        notifyAll();
    }


    public void run() {
        while (true) {
            try {

                DatabaseEvent evt = null;
                synchronized (this) {
                    while (evt == null) {
                        // Return if finished
                        if (finished) {
                            return;
                        }

                        if (event_queue.size() > 0) {
                            // Get the top entry, do we execute it yet?
                            evt = (DatabaseEvent) event_queue.get(0);
                            long diff = evt.time_to_run_event - System.currentTimeMillis();
                            // If we got to wait for the event then do so now...
                            if (diff >= 0) {
                                evt = null;
                                wait((int) diff);
                            }
                        } else {
                            // Event queue empty so wait for someone to put an event on it.
                            wait();
                        }
                    }
                    // Remove the top event from the list,
                    event_queue.remove(0);
                }

                // 'evt' is our event to run,
                evt.runnable.run();

            } catch (Throwable e) {
                system.Debug().write(Lvl.ERROR, this, "SystemDispatchThread error");
                system.Debug().writeException(e);
            }
        }
    }

    // ---------- Inner classes ----------

    static class DatabaseEvent implements Comparable {
        private long time_to_run_event;
        private final Runnable runnable;

        DatabaseEvent(Runnable runnable) {
            this.runnable = runnable;
        }

        public int compareTo(Object ob) {
            DatabaseEvent evt2 = (DatabaseEvent) ob;
            long dif = time_to_run_event - evt2.time_to_run_event;
            if (dif > 0) {
                return 1;
            } else if (dif < 0) {
                return -1;
            }
            return 0;
        }
    }


}
