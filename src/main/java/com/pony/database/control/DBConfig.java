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
