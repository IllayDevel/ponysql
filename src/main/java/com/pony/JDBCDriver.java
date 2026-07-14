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

package com.pony;

/**
 * Instance class that registers the pony JDBC driver with the JDBC
 * Driver Manager.
 * <p>
 * This class now also extends com.pony.database.jdbc.MDriver.
 *
 * @author Tobias Downer
 */

public class JDBCDriver extends com.pony.database.jdbc.MDriver {

    static {
        com.pony.database.jdbc.MDriver.register();
    }

    /**
     * Constructor.
     */
    public JDBCDriver() {
        super();
        // Or we could move driver registering here...
    }

}
