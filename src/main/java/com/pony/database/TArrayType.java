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

/**
 * An implementation of TType for an expression array.
 *
 * @author Tobias Downer
 */

public class TArrayType extends TType {

    static final long serialVersionUID = 6551509064212831922L;

    /**
     * Constructs the type.
     */
    public TArrayType() {
        // There is no SQL type for a query plan node so we make one up here
        super(SQLTypes.ARRAY);
    }

    public boolean comparableTypes(TType type) {
        throw new Error("Query Plan types should not be compared.");
    }

    public int compareObs(Object ob1, Object ob2) {
        throw new Error("Query Plan types should not be compared.");
    }

    public int calculateApproximateMemoryUse(Object ob) {
        return 5000;
    }

    public Class javaClass() {
        return Expression[].class;
    }

}
