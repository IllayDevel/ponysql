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

/**
 * An implementation of TType for a query plan value.
 *
 * @author Tobias Downer
 */

public class TQueryPlanType extends TType {

    static final long serialVersionUID = -1122548450083929179L;

    /**
     * Constructs the type.
     */
    public TQueryPlanType() {
        // There is no SQL type for a query plan node so we make one up here
        super(-19443);
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
        return QueryPlanNode.class;
    }

}
