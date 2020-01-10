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

package com.pony.database.jdbc;

import com.pony.database.control.DBConfig;

import java.sql.SQLException;

/**
 * An interface that is implemented by an object that boots up the database.
 * This is provided as an interface so that we aren't dependant on the
 * entire database when compiling the JDBC code.
 *
 * @author Tobias Downer
 */

public interface LocalBootable {

    /**
     * Attempts to create a new database system with the given name, and the
     * given username/password as the admin user for the system.  Once created,
     * the newly created database will be booted up.
     *
     * @param config the configuration variables.
     */
    DatabaseInterface create(String username, String password,
                             DBConfig config) throws SQLException;

    /**
     * Boots the database with the given configuration.
     *
     * @param config the configuration variables.
     */
    DatabaseInterface boot(DBConfig config) throws SQLException;

    /**
     * Attempts to test if the database exists or not.  Returns true if the
     * database exists.
     *
     * @param config the configuration variables.
     */
    boolean checkExists(DBConfig config) throws SQLException;

    /**
     * Returns true if there is a database currently booted in the current
     * JVM.  Otherwise returns false.
     */
    boolean isBooted() throws SQLException;

    /**
     * Connects this interface to the database currently running in this JVM.
     *
     */
    DatabaseInterface connectToJVM() throws SQLException;

}
