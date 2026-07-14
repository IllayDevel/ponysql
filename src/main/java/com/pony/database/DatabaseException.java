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
 * Exception thrown where various problems occur within the database.
 * <p>
 * @author Tobias Downer
 */

public class DatabaseException extends Exception {

    private final int error_code;

    // ---------- Members ----------

    public DatabaseException(int error_code, String message) {
        super(message);
        this.error_code = error_code;
    }

    public DatabaseException(String message) {
        this(-1, message);
    }

    /**
     * Returns the error code.  -1 means no error code was given.
     */
    public int getErrorCode() {
        return error_code;
    }


}
