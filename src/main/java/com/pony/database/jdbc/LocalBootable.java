/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
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
