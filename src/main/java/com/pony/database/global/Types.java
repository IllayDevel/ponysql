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
 * The possible types used in the database.
 * <p>
 * @author Tobias Downer
 */

public interface Types {

    int DB_UNKNOWN = -1;

    int DB_STRING = 1;
    int DB_NUMERIC = 2;
    int DB_TIME = 3;
    int DB_BINARY = 4;    // @deprecated - use BLOB
    int DB_BOOLEAN = 5;
    int DB_BLOB = 6;
    int DB_OBJECT = 7;

    // This is an extended numeric type that handles neg and positive infinity
    // and NaN.
    int DB_NUMERIC_EXTENDED = 8;

}
