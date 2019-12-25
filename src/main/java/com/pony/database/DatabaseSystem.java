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
import com.pony.database.control.DBConfig;
//import java.io.File;
import java.util.ArrayList;
//import java.util.ResourceBundle;
//import java.util.MissingResourceException;


/**
 * This class provides information about shared resources available for the
 * entire database system running in this VM.  Shared information includes
 * configuration details, DataCellCache, plug-ins, user management, etc.
 *
 * @author Tobias Downer
 */

public final class DatabaseSystem extends TransactionSystem {

    /**
     * The StatementCache that maintains a cache of parsed queries.
     */
    private StatementCache statement_cache = null;

    /**
     * True if all queries on the database should be logged in the 'commands.log'
     * file in the log directory.
     */
    private boolean query_logging;

    /**
     * The WorkerPool object that manages access to the database(s) in the
     * system.
     */
    private WorkerPool worker_pool;

    /**
     * The list of Database objects that this system is being managed by this
     * VM.
     */
    private ArrayList database_list;

    /**
     * Set to true when the database is shut down.
     */
    private boolean shutdown = false;

    /**
     * The UserManager object that handles users connected to the database
     * engine.
     */
    private UserManager user_manager;

    /**
     * The thread to run to shut down the database system.
     */
    private ShutdownThread shutdown_thread;

    /**
     * Constructor.
     */
    public DatabaseSystem() {
        super();
    }

    /**
     * Inits the DatabaseSystem with the configuration properties of the system.
     * This can only be called once, and should be called at database boot time.
     */
    public void init(DBConfig config) {
        super.init(config);

        database_list = new ArrayList();

        // Create the user manager.
        user_manager = new UserManager();

        if (config != null) {

            boolean status;

            // Set up the statement cache.
            status = getConfigBoolean("statement_cache", true);
            if (status) {
                statement_cache = new StatementCache(this, 127, 140, 20);
            }
            Debug().write(Lvl.MESSAGE, DatabaseSystem.class,
                    "statement_cache = " + status);

            // The maximum number of worker threads.
            int max_worker_threads = getConfigInt("maximum_worker_threads", 4);
            if (max_worker_threads <= 0) {
                max_worker_threads = 1;
            }
            Debug().write(Lvl.MESSAGE, DatabaseSystem.class,
                    "Max worker threads set to: " + max_worker_threads);
            worker_pool = new WorkerPool(this, max_worker_threads);

            // Should we be logging commands?
            query_logging = getConfigBoolean("query_logging", false);

        } else {
            throw new Error("Config bundle already set.");
        }

        shutdown = false;

    }


    // ---------- Queries ----------

    /**
     * If query logging is enabled (all queries are output to 'commands.log' in
     * the log directory), this returns true.  Otherwise it returns false.
     */
    public boolean logQueries() {
        return query_logging;
    }


    // ---------- Clean up ----------

    /**
     * Disposes all the resources associated with this DatabaseSystem and
     * invalidates this object.
     */
    public void dispose() {
        super.dispose();
        worker_pool = null;
        database_list = null;
        user_manager = null;
    }

    // ---------- Cache Methods ----------

    /**
     * Returns the StatementCache that is used to cache StatementTree objects
     * that are being queried by the database.  This is used to reduce the
     * SQL command parsing overhead.
     * <p>
     * If this method returns 'null' then statement caching is disabled.
     */
    public StatementCache getStatementCache() {
        return statement_cache;
    }


    // ---------- System preparers ----------

    /**
     * Given a Transaction.CheckExpression, this will prepare the expression and
     * return a new prepared CheckExpression.
     * <p>
     * A DatabaseSystem resolves the variables (ignoring case if necessary) and
     * the functions of the expression.
     */
    public Transaction.CheckExpression prepareTransactionCheckConstraint(
            DataTableDef table_def, Transaction.CheckExpression check) {

        return super.prepareTransactionCheckConstraint(table_def, check);

    }


    // ---------- User management ----------

    /**
     * Returns the UserManager object that handles users that are connected
     * to the database.  The aim of this class is to unify the way users are
     * handled by the engine.  It allows us to perform queries to see who's
     * connected, and any inter-user communication (triggers).
     */
    UserManager getUserManager() {
        return user_manager;
    }

    // ---------- Worker Pool Methods ----------

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
        worker_pool.waitUntilAllWorkersQuiet();
    }

    /**
     * Controls whether the database system is allowed to execute commands or
     * not.  If this is set to true, then calls to 'execute' will be executed
     * as soon as there is a free worker thread available.  Otherwise no
     * commands are executed until this is enabled.
     */
    void setIsExecutingCommands(boolean status) {
        worker_pool.setIsExecutingCommands(status);
    }

    /**
     * Executes database functions from the 'run' method of the given runnable
     * instance on the first available worker thread.  All database functions
     * must go through a worker thread.  If we ensure this, we can easily stop
     * all database functions from executing if need be.  Also, we only need to
     * have a certain number of threads active at any one time rather than a
     * unique thread for each connection.
     */
    void execute(User user, DatabaseConnection database,
                 Runnable runner) {
        worker_pool.execute(user, database, runner);
    }

    // ---------- Shut down methods ----------

    private final ArrayList shut_down_delegates = new ArrayList();

    /**
     * Registers the delegate that is executed when the shutdown thread
     * is activated.  Only one delegate may be registered with the database
     * system.  This is only called once and shuts down the relevant
     * database services.
     */
    void registerShutDownDelegate(Runnable delegate) {
        shut_down_delegates.add(delegate);
    }

    /**
     * The shut down thread.  Started when 'shutDown' is called.
     */
    private class ShutdownThread extends Thread {

        private boolean finished = false;

        synchronized void waitTillFinished() {
            while (finished == false) {
                try {
                    wait();
                } catch (InterruptedException e) {
                }
            }
        }

        public void run() {
            synchronized (this) {
                if (finished) {
                    return;
                }
            }

            // We need this pause so that the command that executed this shutdown
            // has time to exit and retrieve the single row result.
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
            }
            // Stops commands from being executed by the system...
            setIsExecutingCommands(false);
            // Wait until the worker threads are all quiet...
            waitUntilAllWorkersQuiet();

            // Close the worker pool
            worker_pool.shutdown();

            int sz = shut_down_delegates.size();
            if (sz == 0) {
                Debug().write(Lvl.WARNING, this, "No shut down delegates registered!");
            } else {
                for (int i = 0; i < sz; ++i) {
                    Runnable shut_down_delegate = (Runnable) shut_down_delegates.get(i);
                    // Run the shut down delegates
                    shut_down_delegate.run();
                }
                shut_down_delegates.clear();
            }

            synchronized (this) {
                // Wipe all variables from this object
                dispose();

                finished = true;
                notifyAll();
            }
        }
    }

    /**
     * This starts the ShutDown thread that is used to shut down the database
     * server.  Since the actual shutdown method is dependent on the type of
     * database we are running (server or stand-alone) we delegate the
     * shutdown method to the registered shutdown delegate.
     */
    void startShutDownThread() {
        if (!shutdown) {
            shutdown = true;
            shutdown_thread = new ShutdownThread();
            shutdown_thread.start();
        }
    }

    /**
     * Returns true if 'shutDown' method has been called.
     */
    boolean hasShutDown() {
        return shutdown;
    }

    /**
     * Wait until the shutdown thread has completed.  (Shutdown process
     * has finished).
     */
    void waitUntilShutdown() {
        shutdown_thread.waitTillFinished();
    }

}
