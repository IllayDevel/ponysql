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
 * Utility for converting to and from 'Types' objects.
 *
 * @author Tobias Downer
 */

public class TypeUtil {

    /**
     * Converts from a Class object to a type as specified in Types.
     */
    public static int toDBType(Class clazz) {
        if (clazz == String.class) {
            return Types.DB_STRING;
        } else if (clazz == java.math.BigDecimal.class) {
            return Types.DB_NUMERIC;
        } else if (clazz == java.util.Date.class) {
            return Types.DB_TIME;
        } else if (clazz == Boolean.class) {
            return Types.DB_BOOLEAN;
        } else if (clazz == ByteLongObject.class) {
            return Types.DB_BLOB;
        } else {
            return Types.DB_OBJECT;
        }
    }

    /**
     * Converts from a db type to a Class object.
     */
    public static Class toClass(int type) {
        if (type == Types.DB_STRING) {
            return String.class;
        } else if (type == Types.DB_NUMERIC) {
            return java.math.BigDecimal.class;
        } else if (type == Types.DB_TIME) {
            return java.util.Date.class;
        } else if (type == Types.DB_BOOLEAN) {
            return Boolean.class;
        } else if (type == Types.DB_BLOB) {
            return ByteLongObject.class;
        } else if (type == Types.DB_OBJECT) {
            return Object.class;
        } else {
            throw new Error("Unknown type.");
        }
    }


}
