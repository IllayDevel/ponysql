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
