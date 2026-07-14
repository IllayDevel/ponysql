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

import com.pony.database.global.SQLTypes;
import com.pony.database.global.ByteLongObject;

/**
 * An implementation of TType for a java object of possibly defined type.
 *
 * @author Tobias Downer
 */

public class TJavaObjectType extends TType {

    static final long serialVersionUID = -4413863997719593305L;

    /**
     * The type of class this is contrained to or null if it is not constrained
     * to a java class.
     */
    private final String class_type;

    /**
     * Constructs the type.
     */
    public TJavaObjectType(String class_type) {
        super(SQLTypes.JAVA_OBJECT);
        this.class_type = class_type;
    }

    /**
     * Returns the java class type of this type.  For example, "java.net.URL" if
     * this type is constrained to a java.net.URL object.
     */
    public String getJavaClassTypeString() {
        return class_type;
    }

    public boolean comparableTypes(TType type) {
        return (type instanceof TJavaObjectType);
    }

    public int compareObs(Object ob1, Object ob2) {
        throw new Error("Java object types can not be compared.");
    }

    public int calculateApproximateMemoryUse(Object ob) {
        if (ob != null) {
            return ((ByteLongObject) ob).length() + 4;
        } else {
            return 4 + 8;
        }
    }

    public Class javaClass() {
        return ByteLongObject.class;
    }

}
