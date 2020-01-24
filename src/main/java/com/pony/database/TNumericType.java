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

package com.pony.database;

import com.pony.util.BigNumber;

/**
 * An implementation of TType for a number.
 *
 * @author Tobias Downer
 */

public final class TNumericType extends TType {

    static final long serialVersionUID = -5133489773377747175L;

    /**
     * The size of the number.
     */
    private final int size;

    /**
     * The scale of the number.
     */
    private final int scale;


    /**
     * Constructs a type with the given sql_type value, the size,
     * and the scale of the number.  Note that the 'sql_type' MUST be a numeric
     * SQL type (FLOAT, INTEGER, DOUBLE, etc).
     */
    public TNumericType(int sql_type, int size, int scale) {
        super(sql_type);
        this.size = size;
        this.scale = scale;
    }

    /**
     * Returns the size of the number (-1 is don't care).
     */
    public int getSize() {
        return size;
    }

    /**
     * Returns the scale of the number (-1 is don't care).
     */
    public int getScale() {
        return scale;
    }

    // ---------- Implemented from TType ----------

    public boolean comparableTypes(TType type) {
        return (type instanceof TNumericType ||
                type instanceof TBooleanType);
    }

    public int compareObs(Object ob1, Object ob2) {
        BigNumber n1 = (BigNumber) ob1;
        BigNumber n2;

        if (ob2 instanceof BigNumber) {
            n2 = (BigNumber) ob2;
        } else {
            n2 = ob2.equals(Boolean.TRUE) ?
                    BigNumber.BIG_NUMBER_ONE : BigNumber.BIG_NUMBER_ZERO;
        }

        return n1.compareTo(n2);
    }

    public int calculateApproximateMemoryUse(Object ob) {
        // A heuristic - it's difficult to come up with an accurate number
        // for this.
        return 25 + 16;
    }

    public Class javaClass() {
        return BigNumber.class;
    }

}
