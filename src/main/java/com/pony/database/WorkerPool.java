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

import com.pony.debug.DebugLogger;

import java.util.LinkedList;

/**
 * Maintains a pool of worker threads that are used to dispatch commands to
 * a Database sub-system.
 *
 * @author Tobias Downer
 */

final class WorkerPool {

    /**
     * The TransactionSystem that this pool is part of.
     */
    private final TransactionSystem system;

    /**
     * This is the maximum number of worker threads that will be created.
     */
    private int MAXIMUM_WORKER_THREADS = 4;

    /**
     * This is a queue of 'WorkerThread' objects that are currently available
     * to process commands from the service providers.
     */
    private final LinkedList available_worker_threads;

    /**
     * The number of worker threads that have been created in total.
     */
    private int worker_thread_count;

    /**
     * A list of pending Runnable objects that are due to be executed.  This is
     * a queue of events to be run.
     */
    private final LinkedList run_queue;

    /**
     * If this is set to false, then no commands will be executed by the
     * 'execute' method.
     */
    private boolean is_executing_commands;


    /**
     * Constructs the worker thread pool.
     */
    WorkerPool(TransactionSystem system, int max_worker_threads) {
        this.system = system;
        MAXIMUM_WORKER_THREADS = max_worker_threads;

        is_executing_commands = false;

        // Set up the run queue
        run_queue = new LinkedList();
        // Set up the worker threads
        available_worker_threads = new LinkedList();
        worker_thread_count = 0;
//    // Create a single worker thread and start it.
//    ++worker_thread_count;
//    WorkerThread wt = new WorkerThread(this);
//    wt.start();

    }

    /**
     * Returns a DebugLogger object that we can use to log debug messages.
     */
    public final DebugLogger Debug() {
        return system.Debug();
    }

    // ---------- Thread Pooling methods ----------

    /**
     * This is called by a WorkerThread when it is decided that it is ready
     * to service a new command.
     */
    void notifyWorkerReady(WorkerThread worker_thread) {
        synchronized (available_worker_threads) {
            // Add it to the queue of worker threads that are available.
            available_worker_threads.add(worker_thread);

            // Are there any commands pending?
            int q_len = run_queue.size();
            if (q_len > 0) {
                // Execute the bottom element on the queue
                RunCommand command = (RunCommand) run_queue.remove(0);
                execute(command.user, command.database, command.runnable);
            }
        }
    }

    /**
     * This returns the first available WorkerThread object from the thread
     * pool.  If there are no available worker threads available then it returns
     * null.  This method must execute fast and must not block.
     */
    private WorkerThread getFirstWaitingThread() {
        synchronized (available_worker_threads) {
            // Is there a worker thread available?
            int size = available_worker_threads.size();
            if (size > 0) {
                // Yes so remove the first element and return it.
                WorkerThread wt = (WorkerThread) available_worker_threads.remove(0);
                return wt;
            } else {
                // Otherwise create a new worker thread if we can.
                if (worker_thread_count < MAXIMUM_WORKER_THREADS) {
                    ++worker_thread_count;
                    WorkerThread wt = new WorkerThread(this);
                    wt.start();
                    // NOTE: We must _not_ return the worker thread we have just created.
                    //   We must wait until the worker thread has made it self known by
                    //   it calling the 'notifyWorkerReady' method.
                }
                return null;
            }
        }
    }

    /**
     * Executes database functions from the 'run' method of the given runnable
     * instance on a worker thread.  All database functions should go through
     * a worker thread.  If we ensure this, we can easily stop all database
     * functions from executing.  Also, we only need to have a certain number
     * of threads active at any one time rather than a unique thread for each
     * connection.
     */
    void execute(User user, DatabaseConnection database, Runnable runner) {
        synchronized (available_worker_threads) {
            if (is_executing_commands) {
                WorkerThread worker = getFirstWaitingThread();
                if (worker != null) {
//          System.out.println("[Database] executing runner");
                    worker.execute(user, database, runner);
                    return;
                }
            }
//      System.out.println("[Database] adding to run queue");
            RunCommand command = new RunCommand(user, database, runner);
            run_queue.add(command);
        }
    }

    /**
     * Controls whether the database is allowed to execute commands or not.  If
     * this is set to true, then calls to 'execute' will make commands execute.
     */
    void setIsExecutingCommands(boolean status) {
        synchronized (available_worker_threads) {
            if (status == true) {
                is_executing_commands = true;

                // Execute everything on the queue
                for (int i = run_queue.size() - 1; i >= 0; --i) {
                    RunCommand command = (RunCommand) run_queue.remove(i);
                    execute(command.user, command.database, command.runnable);
                }
            } else {
                is_executing_commands = false;
            }
        }
    }

    /**
     * Waits until all executing commands have stopped.  This is best called
     * right after a call to 'setIsExecutingCommands(false)'.  If these two
     * commands are run, the database is in a known state where no commands
     * can be executed.
     * <p>
     * NOTE: This can't be called from the WorkerThread.  Deadlock will
     *   result if we were allowed to do this.
     */
    void waitUntilAllWorkersQuiet() {
        if (Thread.currentThread() instanceof WorkerThread) {
            throw new Error("Can't call this method from a WorkerThread!");
        }

        synchronized (available_worker_threads) {
            // loop until available works = total worker thread count.
            while (worker_thread_count != available_worker_threads.size()) {
                // Wait half a second
                try {
                    available_worker_threads.wait(500);
                } catch (InterruptedException e) {
                }
                // ISSUE: If this lasts for more than 10 minutes, one of the worker
                //   threads is likely in a state of deadlock.  If this happens, we
                //   should probably find all the open worker threads and clean them
                //   up nicely.
            }
        }
    }

    /**
     * Shuts down the WorkerPool object stopping all worker threads.
     */
    void shutdown() {
        synchronized (available_worker_threads) {
            while (available_worker_threads.size() > 0) {
                WorkerThread wt = (WorkerThread) available_worker_threads.remove(0);
                --worker_thread_count;
                wt.shutdown();
            }
        }
    }

    // ---------- Inner classes ----------

    /**
     * Structures within the run_queue list.  This stores the Runnable to
     * run and the User that's executing the command.
     */
    private static final class RunCommand {
        final User user;
        final DatabaseConnection database;
        final Runnable runnable;

        public RunCommand(User user, DatabaseConnection database,
                          Runnable runnable) {
            this.user = user;
            this.database = database;
            this.runnable = runnable;
        }
    }

}
