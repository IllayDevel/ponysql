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

import java.util.Date;

/**
 * An implementation of TType for date objects.
 *
 * @author Tobias Downer
 */

public class TDateType extends TType {

    static final long serialVersionUID = 1494137367081481985L;

    /**
     * Constructs the type.
     */
    public TDateType(int sql_type) {
        super(sql_type);
    }

    public boolean comparableTypes(TType type) {
        return (type instanceof TDateType);
    }

    public int compareObs(Object ob1, Object ob2) {
        return ((Date) ob1).compareTo((Date) ob2);
    }

    public int calculateApproximateMemoryUse(Object ob) {
        return 4 + 8;
    }

    public Class javaClass() {
        return Date.class;
    }

}
