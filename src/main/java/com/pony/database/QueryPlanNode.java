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

import java.util.ArrayList;

/**
 * A node element of a query plan tree.  A plan of a query is represented as
 * a tree structure of such nodes.  The design allows for plan nodes to be
 * easily reorganised for the construction of better plans.
 *
 * @author Tobias Downer
 */

public interface QueryPlanNode extends java.io.Serializable, Cloneable {

    /**
     * Evaluates the node and returns the result as a Table.  The
     * VariableResolver resolves any outer variables
     */
    Table evaluate(QueryContext context, Integer limit);

    Table evaluate(QueryContext context);

    /**
     * Discovers a list of TableName that represent the sources that this query
     * requires to complete itself.  For example, if this is a query plan of
     * two joined table, the fully resolved names of both tables are returned.
     * <p>
     * The resultant list will not contain the same table name more than once.
     * The resultant list contains TableName objects.
     * <p>
     * NOTE, if a table is aliased, the unaliased name is returned.
     */
    ArrayList discoverTableNames(ArrayList list);

    /**
     * Discovers all the correlated variables in the plan (and plan children)
     * that reference a particular layer.  For example, if we wanted to find
     * all the CorrelatedVariable objects that reference the current layer, we
     * would typically call 'discoverCorrelatedVariables(0, new ArrayList())'
     */
    ArrayList discoverCorrelatedVariables(int level, ArrayList list);

    /**
     * Deep clones this query plan.
     */
    Object clone() throws CloneNotSupportedException;

    /**
     * Writes a textural representation of the node to the StringBuffer at the
     * given indent level.
     */
    void debugString(int indent, StringBuffer buf);

}
