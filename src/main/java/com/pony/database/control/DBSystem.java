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

package com.pony.database.control;

import com.pony.database.Database;
import com.pony.database.DatabaseException;
import com.pony.database.jdbc.MConnection;
import com.pony.database.jdbc.DatabaseInterface;
import com.pony.database.jdbcserver.JDBCDatabaseInterface;
import com.pony.debug.*;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * An object used to access and control a single database system running in
 * the current JVM.  This object provides various access methods to
 * safely manipulate the database, as well as allowing server plug-ins.  For
 * example, a TCP/IP JDBC server component might be plugged into this object
 * to open the database to remote access.
 *
 * @author Tobias Downer
 */

public final class DBSystem {

    /**
     * The DBController object.
     */
    private DBController controller;

    /**
     * The DBConfig object that describes the startup configuration of the
     * database.
     */
    private DBConfig config;

    /**
     * The underlying Database object of this system.  This object gives low
     * level access to the system.
     */
    private Database database;

    /**
     * An internal counter for internal connections created on this system.
     */
    private int internal_counter;


    /**
     * Package-protected constructor.
     */
    DBSystem(DBController controller, DBConfig config, Database database) {
        this.controller = controller;
        this.config = config;
        this.database = database;
        this.internal_counter = 0;

        // Register the shut down delegate,
        database.registerShutDownDelegate(() -> internalDispose());

        // Enable commands to the database system...
        database.setIsExecutingCommands(true);

    }

    /**
     * Returns an immutable version of the database system configuration.
     */
    public DBConfig getConfig() {
        return config;
    }

    // ---------- Internal access methods ----------

    /**
     * Returns the com.pony.database.Database object for this control.  This
     * methods only works correctly if the database engine has successfully been
     * initialized.
     * <p>
     * This object is generally not very useful unless you intend to perform
     * some sort of low level function on the database.  This object can be
     * used to bypass the SQL layer and talk directly with the internals of
     * the database.
     *
     * @return a Database object that can be used to access the database system
     *   at a low level.
     */
    public Database getDatabase() {
        return database;
    }

    /**
     * Makes a connection to the database and returns a java.sql.Connection
     * object that can be used to execute queries on the database.  This is a
     * standard connection that talks directly with the database without having
     * to go through any communication protocol layers.
     * <p>
     * For example, if this control is for a Pony database server, the
     * java.sql.Connection returned here does not go through the TCP/IP
     * connection.  For this reason certain database configuration constraints
     * (such as number of concurrent connection on the database) may not apply
     * to this connection.
     * <p>
     * The java.sql.Connection returned here acts exactly as an object returned
     * by a java.sql.MDriver object.
     * <p>
     * An SQLException is thrown if the login fails.
     *
     * @param schema the initial database schema to start the connection in.
     * @param username the user to login to the database under.
     * @param password the password of the user.
     * @throws SQLException if authentication of the user fails.
     * @return a JDBC java.sql.Connection used to access the database.
     */
    public Connection getConnection(String schema,
                                    String username, String password) throws SQLException {

        // Create the host string, formatted as 'Internal/[hash number]/[counter]'
        StringBuffer buf = new StringBuffer();
        buf.append("Internal/");
        buf.append(hashCode());
        buf.append('/');
        synchronized (this) {
            buf.append(internal_counter);
            ++internal_counter;
        }
        String host_string = new String(buf);

        // Create the database interface for an internal database connection.
        DatabaseInterface db_interface =
                new JDBCDatabaseInterface(getDatabase(), host_string);
        // Create the MConnection object (very minimal cache settings for an
        // internal connection).
        MConnection connection = new MConnection("", db_interface, 8, 4092000);
        // Attempt to log in with the given username and password (default schema)
        connection.login(schema, username, password);

        // And return the new connection
        return connection;
    }

    /**
     * Makes a connection to the database and returns a java.sql.Connection
     * object that can be used to execute queries on the database.  This is a
     * standard connection that talks directly with the database without having
     * to go through any communication protocol layers.
     * <p>
     * For example, if this control is for a Pony database server, the
     * java.sql.Connection returned here does not go through the TCP/IP
     * connection.  For this reason certain database configuration constraints
     * (such as number of concurrent connection on the database) may not apply
     * to this connection.
     * <p>
     * The java.sql.Connection returned here acts exactly as an object returned
     * by a java.sql.MDriver object.
     * <p>
     * An SQLException is thrown if the login fails.
     *
     * @param username the user to login to the database under.
     * @param password the password of the user.
     * @throws SQLException if authentication of the user fails.
     * @return a JDBC java.sql.Connection used to access the database.
     */
    public Connection getConnection(String username, String password)
            throws SQLException {
        return getConnection(null, username, password);
    }

    // ---------- Global methods ----------

    /**
     * Sets a flag that causes the database to delete itself from the file system
     * when it is shut down.  This is useful if an application needs a
     * temporary database to work with that is released from the file system
     * when the application ends.
     * <p>
     * By default, a database is not deleted from the file system when it is
     * closed.
     * <p>
     * <b>NOTE: Use with care - setting this flag will cause all data stored
     *    in the database to be lost when the database is shut down.</b>
     */
    public final void setDeleteOnClose(boolean status) {
        database.setDeleteOnShutdown(status);
    }

    /**
     * Closes this database system so it is no longer able to process queries.
     * A database may be shut down either through this method or by executing a
     * query that shuts the system down (for example, 'SHUTDOWN').
     * <p>
     * When a database system is closed, it is not able to be restarted again
     * unless a new DBSystem object is obtained from the DBController.
     * <p>
     * This method also disposes all resources associated with the
     * database system (such as threads, etc) so that it may be reclaimed by
     * the garbage collector.
     * <p>
     * When this method returns this object is no longer usable.
     */
    public void close() {
        if (database != null) {
            database.startShutDownThread();
            database.waitUntilShutdown();
        }
    }

    // ---------- Private methods ----------

    /**
     * Disposes of all the resources associated with this system.  Note that
     * this is private method.  It may only be called from the shutdown
     * delegate registered in the constructor.
     */
    private void internalDispose() {
        if (database != null && database.isInitialized()) {

            // Disable commands (on worker threads) to the database system...
            database.setIsExecutingCommands(false);

            try {
                database.shutdown();
            } catch (DatabaseException e) {
                database.Debug().write(Lvl.ERROR, this,
                        "Unable to shutdown database because of exception");
                database.Debug().writeException(Lvl.ERROR, e);
            }
        }
        controller = null;
        config = null;
        database = null;
    }

}
