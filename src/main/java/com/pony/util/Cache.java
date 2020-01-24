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

package com.pony.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a cache of Objects.  A Cache is similar to a Hashtable, in that
 * you can 'add' and 'get' objects from the container given some key.  However
 * a cache may remove objects from the container when it becomes too full.
 * <p>
 * The cache scheme uses a doubly linked-list hashtable.  The most recently
 * accessed objects are moved to the start of the list.  The end elements in
 * the list are wiped if the cache becomes too full.
 * <p>
 * @author Tobias Downer
 */

public class Cache {

    /**
     * The maximum number of DataCell objects that can be stored in the cache
     * at any one time.
     */
    private int max_cache_size;

    /**
     * The current cache size.
     */
    private int current_cache_size;

    /**
     * The cache HashMap.
     */
    private Map map;

     /**
     * The Constructors.  It takes a maximum size the cache can grow to, and the
     * percentage of the cache that is wiped when it becomes too full.
     */
    public Cache(int hash_size, int max_size, int clean_percentage) {
        Float percentage = (float) clean_percentage/100;
        if (percentage == (float) 0) {
            percentage = 0.75f;
        }
         map = new LinkedHashMap(max_size+1, percentage, true) {
            // This method is called just after a new entry has been added
            public boolean removeEldestEntry(Map.Entry eldest) {
                return size() > max_size;
            }
        };
        max_cache_size = max_size;
        current_cache_size = 0;
    }

    public Cache(int max_size, int clean_percentage) {
        this((max_size * 2) + 1, max_size, 20);
    }

    public Cache(int max_size) {
        this(max_size, 20);
    }

    public Cache() {
        this(50);
    }

    /**
     * Creates the HashMap object to store objects in this cache.  This is
     * available to be overwritten.
     * @deprecated
     */
    protected final int getHashSize() {
        return map.size();
    }


    /**
     * This is called whenever at Object is put into the cache.  This method
     * should determine if the cache should be cleaned and call the clean
     * method if appropriate.
     */
    protected void checkClean() {
        // If we have reached maximum cache size, remove some elements from the
        // end of the list
        if (current_cache_size >= max_cache_size) {
            map.clear();
        }
    }

     // ---------- Public cache methods ----------

    /**
     * Returns the number of nodes that are currently being stored in the
     * cache.
     */
    public final int nodeCount() {
        return current_cache_size;
    }

    /**
     * Puts an Object into the cache with the given key.
     */
    public final void put(Object key, Object ob) {
        // Do we need to clean any cache elements out?
        checkClean();

        map.put(key,ob);

    }

    /**
     * If the cache contains the cell with the given key, this method will
     * return the object.  If the cell is not in the cache, it returns null.
     */
    public final Object get(Object key) {
        return map.get(key);
    }

    /**
     * Ensures that there is no cell with the given key in the cache.  This is
     * useful for ensuring the cache does not contain out-dated information.
     */
    public final Object remove(Object key) {
        Object ob = map.get(key);
        map.remove(key);
        return ob;
    }

    /**
     * Clear the cache of all the entries.
     */
    public void removeAll() {
        map.clear();
    }

    public void clear() {
        removeAll();
    }




}
