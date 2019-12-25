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

package com.pony.database.control;

import java.io.File;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Enumeration;

/**
 * Implements a default database configuration that is useful for setting up
 * a database.  This configuration object is mutable.  Configuration properties
 * can be set by calling the 'setxxx' methods.
 *
 * @author Tobias Downer
 */

public class DefaultDBConfig extends AbstractDBConfig {

    /**
     * Constructs the configuration.
     *
     * @param current_path the current path of the configuration in the file system.  This is
     *   useful if the configuration is based on a file with relative paths set
     *   in it.
     */
    public DefaultDBConfig(File current_path) {
        super(current_path);
    }

    /**
     * Constructs the configuration with the current system path as the
     * configuration path.
     */
    public DefaultDBConfig() {
        this(new File("."));
    }

    /**
     * Gets the default value for the given property value.
     */
    protected String getDefaultValue(String property_key) {
        ConfigProperty property =
                (ConfigProperty) CONFIG_DEFAULTS.get(property_key);
        if (property == null) {
            return null;
        } else {
            return property.getDefaultValue();
        }
    }

    /**
     * Overwrites the configuration key with the given value.
     */
    public void setValue(String property_key, String value) {
        super.setValue(property_key, value);
    }

    /**
     * Loads all the configuration values from the given InputStream.  The
     * input stream must be formatted in a standard properties format.
     */
    public void loadFromStream(InputStream input) throws IOException {
        Properties config = new Properties();
        config.load(new BufferedInputStream(input));
        // For each property in the file
        Enumeration en = config.propertyNames();
        while (en.hasMoreElements()) {
            // Set the property value in this configuration.
            String property_key = (String) en.nextElement();
            setValue(property_key, config.getProperty(property_key));
        }
    }

    /**
     * Loads all the configuration settings from a configuration file.  Useful if
     * you want to load a default configuration from a 'db.conf' file.  The
     * file must be formatted in a standard properties format.
     */
    public void loadFromFile(File configuration_file) throws IOException {
        FileInputStream file_in = new FileInputStream(configuration_file);
        loadFromStream(file_in);
        file_in.close();
    }

    /**
     * Loads all the configuration values from the given URL.  The file must be
     * formatted in a standard properties format.
     */
    public void loadFromURL(URL configuration_url) throws IOException {
        InputStream url_in = configuration_url.openConnection().getInputStream();
        loadFromStream(url_in);
        url_in.close();
    }

    // ---------- Variable helper setters ----------

    /**
     * Sets the path of the database.
     */
    public void setDatabasePath(String path) {
        setValue("database_path", path);
    }

    /**
     * Sets the path of the log.
     */
    public void setLogPath(String path) {
        setValue("log_path", path);
    }

    /**
     * Sets that the engine ignores case for identifiers.
     */
    public void setIgnoreIdentifierCase(boolean status) {
        setValue("ignore_case_for_identifiers", status ? "enabled" : "disabled");
    }

    /**
     * Sets that the database is read only.
     */
    public void setReadOnly(boolean status) {
        setValue("read_only", status ? "enabled" : "disabled");
    }

    /**
     * Sets the minimum debug level for output to the debug log file.
     */
    public void setMinimumDebugLevel(int debug_level) {
        setValue("debug_level", "" + debug_level);
    }


    // ---------- Statics ----------

    /**
     * A Hashtable of default configuration values.  This maps from property_key
     * to ConfigProperty object that describes the property.
     */
    private static final Hashtable CONFIG_DEFAULTS = new Hashtable();

    /**
     * Adds a default property to the CONFIG_DEFAULTS map.
     */
    private static void addDefProperty(ConfigProperty property) {
        CONFIG_DEFAULTS.put(property.getKey(), property);
    }

    static {
        addDefProperty(new ConfigProperty("database_path", "./data", "PATH"));

//    addDefProperty(new ConfigProperty("log_path", "./log", "PATH"));

        addDefProperty(new ConfigProperty("root_path", "jvm", "STRING"));

        addDefProperty(new ConfigProperty("jdbc_server_port", "9157", "STRING"));

        addDefProperty(new ConfigProperty(
                "ignore_case_for_identifiers", "disabled", "BOOLEAN"));

        addDefProperty(new ConfigProperty(
                "regex_library", "gnu.regexp", "STRING"));

        addDefProperty(new ConfigProperty("data_cache_size", "4194304", "INT"));

        addDefProperty(new ConfigProperty(
                "max_cache_entry_size", "8192", "INT"));

        addDefProperty(new ConfigProperty(
                "lookup_comparison_list", "enabled", "BOOLEAN"));

        addDefProperty(new ConfigProperty("maximum_worker_threads", "4", "INT"));

        addDefProperty(new ConfigProperty(
                "dont_synch_filesystem", "disabled", "BOOLEAN"));

        addDefProperty(new ConfigProperty(
                "transaction_error_on_dirty_select", "enabled", "BOOLEAN"));

        addDefProperty(new ConfigProperty("read_only", "disabled", "BOOLEAN"));

        addDefProperty(new ConfigProperty(
                "debug_log_file", "debug.log", "FILE"));

        addDefProperty(new ConfigProperty("debug_level", "20", "INT"));

        addDefProperty(new ConfigProperty(
                "table_lock_check", "enabled", "BOOLEAN"));

    }

    // ---------- Inner classes ----------

    /**
     * An object the describes a single configuration property and the default
     * value for it.
     */
    private static class ConfigProperty {

        private final String key;
        private final String default_value;
        private final String type;
        private final String comment;

        ConfigProperty(String key, String default_value, String type,
                       String comment) {
            this.key = key;
            this.default_value = default_value;
            this.type = type;
            this.comment = comment;
        }

        ConfigProperty(String key, String default_value, String type) {
            this(key, default_value, type, null);
        }

        String getKey() {
            return key;
        }

        String getDefaultValue() {
            return default_value;
        }

        String getType() {
            return type;
        }

        String getComment() {
            return comment;
        }

    }

}
