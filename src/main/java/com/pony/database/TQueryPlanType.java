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
