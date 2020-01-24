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

package com.pony.database.jdbcserver;

import com.pony.database.*;
import com.pony.database.jdbc.*;
import com.pony.debug.*;

import java.sql.SQLException;

/**
 * An implementation of jdbc.DatabaseInterface on the server-side.
 * <p>
 * This receives database commands and dispatches them to the database
 * system.  This assumes that all calls to the methods here are in a
 * UserWorkerThread thread.
 * <p>
 * NOTE: Currently, the client/server use of this object isn't multi-threaded,
 *   however the local connection could be.  Therefore, this object has been
 *   made multi-thread safe.
 *
 * @author Tobias Downer
 */

public class JDBCDatabaseInterface extends AbstractJDBCDatabaseInterface {

    /**
     * Set this to true if command logging is enabled.
     */
    private static final boolean COMMAND_LOGGING = true;

    /**
     * The unique host name denoting the client that's connected.
     */
    private final String host_name;

    /**
     * Sets up the processor.
     */
    public JDBCDatabaseInterface(Database database, String host_name) {
        super(database);
        this.host_name = host_name;
    }

    /**
     * Tries to authenticate the username and password against the given
     * database.  Returns true if we are successful.  If successful, alters the
     * state of this object to reflect the fact the user has logged in.
     */
    private boolean authenticate(Database database, String default_schema,
                                 String username, String password,
                                 final DatabaseCallBack database_call_back) {

        // If the 'user' variable is null, no one is currently logged in to this
        // connection.

        if (getUser() == null) {

            if (COMMAND_LOGGING && database.getSystem().logQueries()) {
                // Output the instruction to the commands log.
                StringBuffer log_str = new StringBuffer();
                log_str.append("[JDBC] [");
                log_str.append(username);
                log_str.append("] ");
                log_str.append('[');
                log_str.append(host_name);
                log_str.append("] ");
                log_str.append("Log in.\n");
                database.getCommandsLog().log(new String(log_str));
            }

            // Write debug message,
            if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                Debug().write(Lvl.INFORMATION, this,
                        "Authenticate User: " + username);
            }

            // Create a UserCallBack class.
            DatabaseConnection.CallBack call_back =
                    (trigger_name, trigger_event, trigger_source, fire_count) -> {
                        StringBuffer message = new StringBuffer();
                        message.append(trigger_name);
                        message.append(' ');
                        message.append(trigger_source);
                        message.append(' ');
                        message.append(fire_count);

                        database_call_back.databaseEvent(99, new String(message));
                    };

            // Try to create a User object.
            User this_user = database.authenticateUser(username, password,
                    host_name);
            DatabaseConnection database_connection = null;

            // If successful, ask the engine for a DatabaseConnection object.
            if (this_user != null) {
                database_connection =
                        database.createNewConnection(this_user, call_back);

                // Put the connection in exclusive mode
                LockingMechanism locker = database_connection.getLockingMechanism();
                locker.setMode(LockingMechanism.EXCLUSIVE_MODE);
                try {

                    // By default, JDBC connections are auto-commit
                    database_connection.setAutoCommit(true);

                    // Set the default schema for this connection if it exists
                    if (database_connection.schemaExists(default_schema)) {
                        database_connection.setDefaultSchema(default_schema);
                    } else {
                        Debug().write(Lvl.WARNING, this,
                                "Couldn't change to '" + default_schema + "' schema.");
                        // If we can't change to the schema then change to the APP schema
                        database_connection.setDefaultSchema("APP");
                    }

                } finally {
                    try {
                        // Make sure we commit the connection.
                        database_connection.commit();
                    } catch (TransactionException e) {
                        // Just issue a warning...
                        Debug().writeException(Lvl.WARNING, e);
                    } finally {
                        // Guarentee that we unluck from EXCLUSIVE
                        locker.finishMode(LockingMechanism.EXCLUSIVE_MODE);
                    }
                }

            }

            // If we have a user object, then init the object,
            if (this_user != null) {
                init(this_user, database_connection);
                return true;
            } else {
                // Otherwise, return false.
                return false;
            }

        } else {
            throw new RuntimeException("Attempt to authenticate user twice");
        }

    }

    // ---------- Implemented from DatabaseInterface ----------

    public boolean login(String default_schema, String username, String password,
                         DatabaseCallBack database_call_back)
            throws SQLException {

        Database database = getDatabase();

        boolean b = authenticate(database, default_schema, username, password,
                database_call_back);
        return b;
    }


    public QueryResponse execQuery(SQLQuery query) throws SQLException {

        // Check the interface isn't disposed (connection was closed).
        checkNotDisposed();

        User user = getUser();
        DatabaseConnection database_connection = getDatabaseConnection();

        // Log this query if query logging is enabled
        if (COMMAND_LOGGING && getDatabase().getSystem().logQueries()) {
            // Output the instruction to the commands log.
            StringBuffer log_str = new StringBuffer();
            log_str.append("[JDBC] [");
            log_str.append(user.getUserName());
            log_str.append("] ");
            log_str.append('[');
            log_str.append(host_name);
            log_str.append("] ");
            log_str.append("Query: ");
            log_str.append(query.getQuery());
            log_str.append('\n');
            user.getDatabase().getCommandsLog().log(new String(log_str));
        }

        // Write debug message (INFORMATION level)
        if (Debug().isInterestedIn(Lvl.INFORMATION)) {
            Debug().write(Lvl.INFORMATION, this,
                    "Query From User: " + user.getUserName() + "@" + host_name);
            Debug().write(Lvl.INFORMATION, this,
                    "Query: " + query.getQuery().trim());
        }

        // Get the locking mechanism.
        LockingMechanism locker = database_connection.getLockingMechanism();
        int lock_mode = -1;
        QueryResponse response = null;
        try {
            try {

                // For simplicity - all database locking is now exclusive inside
                // a transaction.  This means it is not possible to execute
                // queries concurrently inside a transaction.  However, we are
                // still able to execute queries concurrently from different
                // connections.
                //
                // It's debatable whether we even need to perform this lock anymore
                // because we could change the contract of this method so that
                // it is not thread safe.  This would require that the callee ensures
                // more than one thread can not execute queries on the connection.
                lock_mode = LockingMechanism.EXCLUSIVE_MODE;
                locker.setMode(lock_mode);

                // Execute the query (behaviour for this comes from super).
                response = super.execQuery(query);

                // Return the result.
                return response;

            } finally {
                try {
                    // This is executed no matter what happens.  Very important we
                    // unlock the tables.
                    if (lock_mode != -1) {
                        locker.finishMode(lock_mode);
                    }
                } catch (Throwable e) {
                    // If this throws an exception, we should output it to the debug
                    // log and screen.
                    e.printStackTrace(System.err);
                    Debug().write(Lvl.ERROR, this, "Exception finishing locks");
                    Debug().writeException(e);
                    // Note, we can't throw an error here because we may already be in
                    // an exception that happened in the above 'try' block.
                }
            }

        } finally {
            // This always happens after tables are unlocked.
            // Also guarenteed to happen even if something fails.

            // If we are in auto-commit mode then commit the query here.
            // Do we auto-commit?
            if (database_connection.getAutoCommit()) {
                // Yes, so grab an exclusive lock and auto-commit.
                try {
                    // Lock into exclusive mode.
                    locker.setMode(LockingMechanism.EXCLUSIVE_MODE);
                    // If an error occured then roll-back
                    if (response == null) {
                        // Rollback.
                        database_connection.rollback();
                    } else {
                        try {
                            // Otherwise commit.
                            database_connection.commit();
                        } catch (Throwable e) {
                            // Dispose this response if the commit failed.
                            disposeResult(response.getResultID());
                            // And throw the SQL Exception
                            throw handleExecuteThrowable(e, query);
                        }
                    }
                } finally {
                    locker.finishMode(LockingMechanism.EXCLUSIVE_MODE);
                }
            }

        }

    }


    public void dispose() throws SQLException {
        if (getUser() != null) {
            DatabaseConnection database = getDatabaseConnection();
            LockingMechanism locker = database.getLockingMechanism();
            try {
                // Lock into exclusive mode,
                locker.setMode(LockingMechanism.EXCLUSIVE_MODE);
                // Roll back any open transaction.
                database.rollback();
            } finally {
                // Finish being in exclusive mode.
                locker.finishMode(LockingMechanism.EXCLUSIVE_MODE);
                // Close the database connection object.
                database.close();
                // Log out the user
                getUser().logout();
                // Call the internal dispose method.
                internalDispose();
            }
        }
    }

}
