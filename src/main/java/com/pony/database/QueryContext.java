/*
 * Pony SQL Database ( http://i-devel.ru )
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
