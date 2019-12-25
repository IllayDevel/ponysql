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

/**
 * An implementation of TType that represents a NULL type.  A Null type is
 * an object that can't be anything else except null.
 *
 * @author Tobias Downer
 */

public class TNullType extends TType {

    static final long serialVersionUID = -271824967935043427L;

    /**
     * Constructs the type.
     */
    public TNullType() {
        // There is no SQL type for a query plan node so we make one up here
        super(SQLTypes.NULL);
    }

    public boolean comparableTypes(TType type) {
        return (type instanceof TNullType);
    }

    public int compareObs(Object ob1, Object ob2) {
        // It's illegal to compare NULL types with this method so we throw an
        // exception here (see method specification).
        throw new Error("compareObs can not compare NULL types.");
    }

    public int calculateApproximateMemoryUse(Object ob) {
        return 16;
    }

    public Class javaClass() {
        return Object.class;
    }

}
