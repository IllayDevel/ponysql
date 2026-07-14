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

/**
 * Contant static values that determine several parameters of the database
 * operation.  It is important that a database data generated from a
 * compilation from one set of constants is not used with the same database
 * with different constants.
 * <p>
 * @author Tobias Downer
 */

public interface DatabaseConstants {

    /**
     * The maximum length in characters of the string that represents the name
     * of the database.
     */
    int MAX_DATABASE_NAME_LENGTH = 50;

    /**
     * The maximum length in characters of the string that represents the name
     * of a privaledge group.
     */
    int MAX_PRIVGROUP_NAME_LENGTH = 50;

    /**
     * The maximum length in characters of the string that holds the table
     * name.  The table name is used to reference a Table object in a Database.
     */
    int MAX_TABLE_NAME_LENGTH = 50;

    /**
     * The maximum length in characters of the string that holds the user
     * name.  The user name is used in many security and priviledge operations.
     */
    int MAX_USER_NAME_LENGTH = 50;

    /**
     * The maximum length in character of the string that holds a users
     * password.  The password is used when logging into the database.
     */
    int MAX_PASSWORD_LENGTH = 80;

}
