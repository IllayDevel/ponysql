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

package com.pony.database;

import java.sql.Connection;

/**
 * An interface for accessing a database connection inside a stored procedure.
 *
 * @author Tobias Downer
 */

public interface ProcedureConnection {

    /**
     * Returns a JDBC Connection implementation for executing queries on this
     * connection.  The Connection has auto-commit turned off, and it
     * disables the ability for the connection to 'commit' changes to the
     * database.
     * <p>
     * This method is intended to provide the procedure developer with a
     * convenient and consistent way to query and manipulate the database from
     * the body of a stored procedure method.
     * <p>
     * The java.sql.Connection object returned here may invalidate when the
     * procedure invokation call ends so the returned object must not be cached
     * to be used again later.
     * <p>
     * The returned java.sql.Connection object is NOT thread safe and should
     * only be used by a single thread.  Accessing this connection from multiple
     * threads will result in undefined behaviour.
     * <p>
     * The Connection object returned here has the same privs as the user who
     * owns the stored procedure.
     */
    Connection getJDBCConnection();

    /**
     * Returns the Database object for this database providing access to various
     * general database features including backing up replication and
     * configuration.  Some procedures may not be allowed access to this object
     * in which case a ProcedureException is thrown notifying of the security
     * violation.
     */
    Database getDatabase();

}

