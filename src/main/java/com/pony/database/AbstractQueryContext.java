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
