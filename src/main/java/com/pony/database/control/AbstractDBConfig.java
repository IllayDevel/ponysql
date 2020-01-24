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

package com.pony.database.control;

import java.io.File;
import java.util.Hashtable;

/**
 * An abstract implementation of DBConfig.
 *
 * @author Tobias Downer
 */

public class AbstractDBConfig implements DBConfig {

    /**
     * The current base path of the database configuration.
     */
    private final File current_path;

    /**
     * The Hashtable mapping from configuration key to value for the key.
     */
    private Hashtable<Object,Object> key_map;

    /**
     * Constructs the DBConfig.
     */
    public AbstractDBConfig(File current_path) {
        this.current_path = current_path;
        this.key_map = new Hashtable<>();
    }

    /**
     * Returns the default value for the configuration property with the given
     * key.
     */
    protected String getDefaultValue(String property_key) {
        // This abstract implementation returns null for all default keys.
        return null;
    }

    /**
     * Sets the configuration value for the key property key.
     */
    protected void setValue(String property_key, String val) {
        key_map.put(property_key, val);
    }

    // ---------- Implemented from DBConfig ----------

    public File currentPath() {
        return current_path;
    }

    public String getValue(String property_key) {
        // If the key is in the map, return it here
        String val = (String) key_map.get(property_key);
        if (val == null) {
            return getDefaultValue(property_key);
        }
        return val;
    }

    public DBConfig immutableCopy() {
        AbstractDBConfig immutable_copy = new AbstractDBConfig(current_path);
        immutable_copy.key_map = (Hashtable<Object,Object>) key_map.clone();
        return immutable_copy;
    }

}
