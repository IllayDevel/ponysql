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

import java.util.HashMap;

/**
 * An abstract implementation of QueryContext
 *
 * @author Tobias Downer
 */

public abstract class AbstractQueryContext implements QueryContext {

    /**
     * Any marked tables that are made during the evaluation of a query plan.
     * (String) -> (Table)
     */
    private HashMap<Object,Object> marked_tables;


    /**
     * Marks a table in a query plan.
     */
    public void addMarkedTable(String mark_name, Table table) {
        if (marked_tables == null) {
            marked_tables = new HashMap<>();
        }
        marked_tables.put(mark_name, table);
    }

    /**
     * Returns a table that was marked in a query plan or null if no mark was
     * found.
     */
    public Table getMarkedTable(String mark_name) {
        if (marked_tables == null) {
            return null;
        }
        return (Table) marked_tables.get(mark_name);
    }

    /**
     * Put a Table into the cache.
     */
    public void putCachedNode(long id, Table table) {
        if (marked_tables == null) {
            marked_tables = new HashMap<>();
        }
        marked_tables.put(id, table);
    }

    /**
     * Returns a cached table or null if it isn't cached.
     */
    public Table getCachedNode(long id) {
        if (marked_tables == null) {
            return null;
        }
        return (Table) marked_tables.get(id);
    }

    /**
     * Clears the cache of any cached tables.
     */
    public void clearCache() {
        if (marked_tables != null) {
            marked_tables.clear();
        }
    }

}
