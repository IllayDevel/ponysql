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

import com.pony.util.Cache;
import com.pony.database.global.ObjectTranslator;
import com.pony.database.global.ByteLongObject;
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
