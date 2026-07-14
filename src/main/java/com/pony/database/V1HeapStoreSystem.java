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

import com.pony.store.*;

import java.io.IOException;
import java.util.HashMap;

/**
 * An implementation of StoreSystem that stores all persistent data on the
 * heap using HeapStore objects.
 *
 * @author Tobias Downer
 */

class V1HeapStoreSystem implements StoreSystem {

    /**
     * A mapping from name to Store object for this heap store system.
     */
    private final HashMap name_store_map;

    /**
     * A mapping from Store object to name.
     */
    private final HashMap store_name_map;


    /**
     * Constructor.
     */
    V1HeapStoreSystem() {
        name_store_map = new HashMap();
        store_name_map = new HashMap();
    }


    public boolean storeExists(String name) {
        return (name_store_map.get(name) != null);
    }

    public Store createStore(String name) {
        if (!storeExists(name)) {
            HeapStore store = new HeapStore();
            name_store_map.put(name, store);
            store_name_map.put(store, name);
            return store;
        } else {
            throw new RuntimeException("Store exists: " + name);
        }
    }

    public Store openStore(String name) {
        HeapStore store = (HeapStore) name_store_map.get(name);
        if (store == null) {
            throw new RuntimeException("Store does not exist: " + name);
        }
        return store;
    }

    public boolean closeStore(Store store) {
        if (store_name_map.get(store) == null) {
            throw new RuntimeException("Store does not exist.");
        }
        return true;
    }

    public boolean deleteStore(Store store) {
        String name = (String) store_name_map.remove(store);
        name_store_map.remove(name);
        return true;
    }

    public void setCheckPoint() {
        // Check point logging not necessary with heap store
    }

    // ---------- Locking ----------

    public void lock(String lock_name) throws IOException {
        // Not required because heap memory is not a shared resource that can be
        // accessed by multiple JVMs
    }

    public void unlock(String lock_name) throws IOException {
        // Not required because heap memory is not a shared resource that can be
        // accessed by multiple JVMs
    }

}

