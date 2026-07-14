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
