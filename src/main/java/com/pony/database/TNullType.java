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
