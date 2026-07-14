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

package com.pony.database.global;

/**
 * This class contains a number of standard messages that are displayed
 * throughout the operation of the database.  They are put into a single class
 * to allow for easy future modification.
 *
 * @author Tobias Downer
 */

public final class StandardMessages {

    /**
     * The name of the author (me).
     */
    public static String AUTHOR = "IllayDevel";

    /**
     * The standard copyright message.
     */
    public static final String COPYRIGHT =
            "Copyright (C) 2019-2020 IllayDevel " +
                    "All rights reserved.";

    /**
     * The global version number of the database system.
     */
    public static final String VERSION = "1.0.6";

    /**
     * The global name of the system.
     */
    public static final String NAME = "Pony SQL Database ( " + VERSION + " )";

}
