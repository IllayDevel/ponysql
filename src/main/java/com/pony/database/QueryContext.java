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
 * Facts about a particular query including the root table sources, user name
 * of the controlling context, sequence state, etc.
 *
 * @author Tobias Downer
 */

public interface QueryContext {

    /**
     * Returns a TransactionSystem object that is used to determine information
     * about the transactional system.
     */
    TransactionSystem getSystem();

    /**
     * Returns the user name of the connection.
     */
    String getUserName();

    /**
     * Returns a FunctionLookup object used to convert FunctionDef objects to
     * Function objects when evaluating an expression.
     */
    FunctionLookup getFunctionLookup();

    // ---------- Sequences ----------

    /**
     * Increments the sequence generator and returns the next unique key.
     */
    long nextSequenceValue(String generator_name);

    /**
     * Returns the current sequence value returned for the given sequence
     * generator within the connection defined by this context.  If a value was
     * not returned for this connection then a statement exception is generated.
     */
    long currentSequenceValue(String generator_name);

    /**
     * Sets the current sequence value for the given sequence generator.
     */
    void setSequenceValue(String generator_name, long value);

    // ---------- Caching ----------

    /**
     * Marks a table in a query plan.
     */
    void addMarkedTable(String mark_name, Table table);

    /**
     * Returns a table that was marked in a query plan or null if no mark was
     * found.
     */
    Table getMarkedTable(String mark_name);

    /**
     * Put a Table into the cache.
     */
    void putCachedNode(long id, Table table);

    /**
     * Returns a cached table or null if it isn't cached.
     */
    Table getCachedNode(long id);

    /**
     * Clears the cache of any cached tables.
     */
    void clearCache();

}
