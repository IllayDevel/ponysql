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

import java.sql.Connection;
import java.sql.SQLException;

import com.pony.database.jdbc.MConnection;
import com.pony.database.jdbc.DatabaseCallBack;
import com.pony.database.jdbcserver.AbstractJDBCDatabaseInterface;

/**
 * Helper and convenience methods and classes for creating a JDBC interface
 * that has direct access to an open transaction of a DatabaseConnection.
 * This class allows us to provide JDBC access to stored procedures from
 * inside the engine.
 *
 * @author Tobias Downer
 */

class InternalJDBCHelper {

    /**
     * Returns a java.sql.Connection object that is bound to the given
     * DatabaseConnection object.  Queries executed on the Connection alter
     * the currently open transaction.
     * <p>
     * Note: It is assumed that the DatabaseConnection is locked in exclusive
     *   mode when a query is executed (eg. via the 'executeXXX' methods in
     *   Statement).
     * <p>
     * Note: Auto-commit is <b>DISABLED</b> for the SQL connection and can not
     *   be enabled.
     */
    static Connection createJDBCConnection(User user,
                                           DatabaseConnection connection) {
        InternalDatabaseInterface db_interface =
                new InternalDatabaseInterface(user, connection);
        return new InternalConnection(connection, db_interface, 11, 4092000);
    }

    /**
     * Disposes the JDBC Connection object returned by the 'createJDBCConnection'
     * method.  This should be called to free resources associated with the
     * connection object.
     * <p>
     * After this has completed the given Connection object in invalidated.
     */
    static void disposeJDBCConnection(Connection jdbc_connection)
            throws SQLException {
        InternalConnection connection = (InternalConnection) jdbc_connection;
        // Dispose the connection.
        connection.internalClose();
    }


    // ---------- Inner classes ----------

    /**
     * A derived java.sql.Connection class from MConnection.  This class disables
     * auto commit, and inherits case insensitivity from the parent
     * DatabaseConnection.
     * <p>
     * The decision to disable auto-commit was because this connection will
     * typically be used as a sub-process for executing a complete command.
     * Disabling auto-commit makes handling an internal connection more user
     * friendly.  Also, toggling this flag in the DatabaseConnection in mid-
     * command is probably a very bad idea.
     */
    private static class InternalConnection extends MConnection {

        /**
         * The DatabaseInterface for this connection.
         */
        private final InternalDatabaseInterface internal_db_interface;

        /**
         * Constructs the internal java.sql.Connection.
         */
        public InternalConnection(DatabaseConnection db,
                                  InternalDatabaseInterface jdbc_interface,
                                  int cache_size, int max_size) {
            super("", jdbc_interface, cache_size, max_size);
            internal_db_interface = jdbc_interface;
            setCaseInsensitiveIdentifiers(db.isInCaseInsensitiveMode());
        }

        /**
         * Returns the InternalDatabaseInterface that is used in this
         * connection.
         */
        InternalDatabaseInterface getDBInterface() {
            return internal_db_interface;
        }

        /**
         * Overwritten from MConnection - auto-commit is disabled and can not be
         * enabled.
         */
        public void setAutoCommit(boolean status) throws SQLException {
            if (status == true) {
                throw new SQLException(
                        "Auto-commit can not be enabled for an internal connection.");
            }
        }

        /**
         * Overwritten from MConnection - auto-commit is disabled and can not be
         * enabled.
         */
        public boolean getAutoCommit() throws SQLException {
            return false;
        }

        /**
         * Overwritten from MConnection - closing an internal connection is a
         * no-op.  An InternalConnection should only close when the underlying
         * transaction closes.
         * <p>
         * To dispose an InternalConnection, use the static
         * 'disposeJDBCConnection' method.
         */
        public void close() {
            // IDEA: Perhaps we should use this as a hint to clear some caches
            //   and free up some memory.
        }

    }

    /**
     * An implementation of DatabaseInterface used to execute queries on the
     * DatabaseConnection and return results to the JDBC client.
     * <p>
     * This is a thin implementation of jdbcserver.AbstractJDBCDatabaseInterface.
     */
    private static class InternalDatabaseInterface
            extends AbstractJDBCDatabaseInterface {

        /**
         * Constructor.
         */
        public InternalDatabaseInterface(User user, DatabaseConnection db) {
            super(db.getDatabase());
            init(user, db);
        }

        // ---------- Implemented from DatabaseInterface ----------

        public boolean login(String default_schema,
                             String username, String password,
                             DatabaseCallBack call_back) throws SQLException {
            // This should never be used for an internal connection.
            throw new SQLException(
                    "'login' is not supported for InterfaceDatabaseInterface");
        }

        public void dispose() throws SQLException {
            internalDispose();
        }

    }

}
