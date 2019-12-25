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

import java.util.*;

/**
 * A HashMap that maps from a source to a list of items for that source.  This
 * is useful as a searching mechanism where the list of searched items are
 * catagorised in the mapped list.
 *
 * @author Tobias Downer
 */

public class HashMapList {

    private static final List EMPTY_LIST = Arrays.asList(new Object[0]);

    private final HashMap map;

    /**
     * Constructs the map.
     */
    public HashMapList() {
        map = new HashMap();
    }

    /**
     * Puts a value into the map list.
     */
    public void put(Object key, Object val) {
        ArrayList list = (ArrayList) map.get(key);
        if (list == null) {
            list = new ArrayList();
        }
        list.add(val);
        map.put(key, list);
    }

    /**
     * Returns the list of values that are in the map under this key.  Returns
     * an empty list if no key map found.
     */
    public List get(Object key) {
        ArrayList list = (ArrayList) map.get(key);
        if (list != null) {
            return list;
        }
        return EMPTY_LIST;
    }

    /**
     * Removes the given value from the list with the given key.
     */
    public boolean remove(Object key, Object val) {
        ArrayList list = (ArrayList) map.get(key);
        if (list == null) {
            return false;
        }
        boolean status = list.remove(val);
        if (list.size() == 0) {
            map.remove(key);
        }
        return status;
    }

    /**
     * Clears the all the values for the given key.  Returns the List of
     * items that were stored under this key.
     */
    public List clear(Object key) {
        ArrayList list = (ArrayList) map.remove(key);
        if (list == null) {
            return new ArrayList();
        }
        return list;
    }

    /**
     * The Set of all keys.
     */
    public Set keySet() {
        return map.keySet();
    }

    /**
     * Returns true if the map contains the key.
     */
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

}
