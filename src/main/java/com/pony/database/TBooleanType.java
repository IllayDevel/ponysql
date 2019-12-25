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

import com.pony.util.BigNumber;

/**
 * An implementation of TType for a boolean value.
 *
 * @author Tobias Downer
 */

public final class TBooleanType extends TType {

    static final long serialVersionUID = 5602396246537490259L;

    /**
     * Constructs the type.
     */
    public TBooleanType(int sql_type) {
        super(sql_type);
    }

    public boolean comparableTypes(TType type) {
        return (type instanceof TBooleanType ||
                type instanceof TNumericType);
    }

    public int compareObs(Object ob1, Object ob2) {

        if (ob2 instanceof BigNumber) {
            BigNumber n2 = (BigNumber) ob2;
            BigNumber n1 = ob1.equals(Boolean.FALSE) ?
                    BigNumber.BIG_NUMBER_ZERO : BigNumber.BIG_NUMBER_ONE;
            return n1.compareTo(n2);
        }

        if (ob1 == ob2 || ob1.equals(ob2)) {
            return 0;
        } else if (ob1.equals(Boolean.TRUE)) {
            return 1;
        } else {
            return -1;
        }
    }

    public int calculateApproximateMemoryUse(Object ob) {
        return 5;
    }


    public Class javaClass() {
        return Boolean.class;
    }

}
