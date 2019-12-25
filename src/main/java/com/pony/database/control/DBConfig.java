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

/**
 * A container object of configuration details of a database system.  This
 * object can be used to programmatically setup configuration properies
 * in a database system.
 *
 * @author Tobias Downer
 */

public interface DBConfig {

    /**
     * Returns the current path set for this configuration.  This is
     * useful if the configuration is based on a configuration file that has
     * path references relative to the configuration file.  In this case,
     * the path returned here would be the path to the configuration
     * file.
     */
    File currentPath();

    /**
     * Returns the value that was set for the configuration property with the
     * given name.
     * <p>
     * This method must always returns a value that the database engine can use
     * provided the 'property_key' is a supported key.  If the property key
     * is not supported and the key was not set, null is returned.
     */
    String getValue(String property_key);

    /**
     * Makes an immutable copy of this configuration.
     */
    DBConfig immutableCopy();

}
