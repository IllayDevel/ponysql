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
 * Thrown when a transaction error happens.  This can only be thrown during
 * the commit process of a transaction.
 *
 * @author Tobias Downer
 */

public class TransactionException extends Exception {

    // The types of transaction exceptions.

    /**
     * Thrown when a transaction deletes or updates a row that another
     * transaction has committed a change to.
     */
    public final static int ROW_REMOVE_CLASH = 1;

    /**
     * Thrown when a transaction drops or alters a table that another transaction
     * has committed a change to.
     */
    public final static int TABLE_REMOVE_CLASH = 2;

    /**
     * Thrown when a transaction adds/removes/modifies rows from a table that
     * has been dropped by another transaction.
     */
    public final static int TABLE_DROPPED = 3;

    /**
     * Thrown when a transaction selects data from a table that has committed
     * changes to it from another transaction.
     */
    public final static int DIRTY_TABLE_SELECT = 4;

    /**
     * Thrown when a transaction conflict occurs and would cause duplicate tables
     * to be created.
     */
    public final static int DUPLICATE_TABLE = 5;


    /**
     * The type of error.
     */
    private final int type;

    public TransactionException(int type, String message) {
        super(message);
        this.type = type;
    }

    /**
     * Returns the type of transaction error this is.
     */
    public int getType() {
        return type;
    }


}
