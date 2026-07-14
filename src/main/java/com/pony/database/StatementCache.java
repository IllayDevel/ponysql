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

import com.pony.util.Cache;
import com.pony.debug.*;

/**
 * A cache that maintains a serialized set of StatementTree objects that can
 * be deserialized on demand.  The purpose of this cache is to improve the
 * performance of queries that are run repeatedly (for example, multiple
 * INSERT statements).
 * <p>
 * SYNCHRONIZATION: This object is safe to use over multiple threads.
 *
 * @author Tobias Downer
 */

public final class StatementCache {

    /**
     * The DatabaseSystem of this cache.
     */
    private final DatabaseSystem system;

    /**
     * The internal cache representation.
     */
    private final Cache cache;

    /**
     * Constructs the cache.
     */
    public StatementCache(DatabaseSystem system,
                          int hash_size, int max_size, int clean_percentage) {
        this.system = system;
        cache = new Cache(hash_size, max_size, clean_percentage);
    }

    /**
     * Returns a DebugLogger object we can use to log debug messages.
     */
    public final DebugLogger Debug() {
        return system.Debug();
    }

    /**
     * Puts a new query string/StatementTree into the cache.
     */
    public synchronized void put(String query_string,
                                 StatementTree statement_tree) {
        query_string = query_string.trim();
        // Is this query string already in the cache?
        if (cache.get(query_string) == null) {
            try {
                Object cloned_tree = statement_tree.clone();
                cache.put(query_string, cloned_tree);
            } catch (CloneNotSupportedException e) {
                Debug().writeException(e);
                throw new Error("Unable to clone statement tree: " + e.getMessage());
            }
        }
    }

    /**
     * Gets a StatementTree for the query string if it is stored in the cache.
     * If it isn't stored in the cache returns null.
     */
    public synchronized StatementTree get(String query_string) {
        query_string = query_string.trim();
        Object ob = cache.get(query_string);
        if (ob != null) {
            try {
//        System.out.println("CACHE HIT!");
                // We found a cached version of this query so deserialize and return
                // it.
                StatementTree cloned_tree = (StatementTree) ob;
                return (StatementTree) cloned_tree.clone();
            } catch (CloneNotSupportedException e) {
                Debug().writeException(e);
                throw new Error("Unable to clone statement tree: " + e.getMessage());
            }
        }
        // Not found so return null
        return null;
    }

}
