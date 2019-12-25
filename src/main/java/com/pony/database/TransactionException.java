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
