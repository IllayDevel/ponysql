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
