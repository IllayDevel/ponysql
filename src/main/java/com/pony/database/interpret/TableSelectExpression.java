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

package com.pony.database.interpret;

import com.pony.database.*;

import java.util.*;

/**
 * A container object for the a table select expression, eg.
 * <p><pre>
 *               SELECT [columns]
 *                 FROM [tables]
 *                WHERE [search_clause]
 *             GROUP BY [column]
 *               HAVING [search_clause]
 * [composite_function] [table_select_expression]
 * </pre><p>
 * Note that a TableSelectExpression can be nested in the various clauses of
 * this object.
 *
 * @author Tobias Downer
 */

public final class TableSelectExpression
        implements java.io.Serializable, StatementTreeObject, Cloneable {

    static final long serialVersionUID = 6946017316981412561L;

    /**
     * True if we only search for distinct elements.
     */
    public boolean distinct = false;

    /**
     * The list of columns to select from.
     * (SelectColumn)
     */
    public ArrayList columns = new ArrayList();

    /**
     * The from clause.
     */
    public FromClause from_clause = new FromClause();

    /**
     * The where clause.
     */
    public SearchExpression where_clause = new SearchExpression();
	
	/**
     * The limit clause.
     */
    public Integer limit_clause = -1;


    /**
     * The list of columns to group by.
     * (ByColumn)
     */
    public ArrayList group_by = new ArrayList();

    /**
     * The group max variable or null if no group max.
     */
    public Variable group_max = null;

    /**
     * The having clause.
     */
    public SearchExpression having_clause = new SearchExpression();


    /**
     * If there is a composite function this is set to the composite enumeration
     * from CompositeTable.
     */
    int composite_function = -1;  // (None)

    /**
     * If this is an ALL composite (no removal of duplicate rows) it is true.
     */
    boolean is_composite_all;

    /**
     * The composite table itself.
     */
    TableSelectExpression next_composite;

    /**
     * Constructor.
     */
    public TableSelectExpression() {
    }

    /**
     * Chains a new composite function to this expression.  For example, if
     * this expression is a UNION ALL with another expression it would be
     * set through this method.
     */
    public void chainComposite(TableSelectExpression expression,
                               String composite, boolean is_all) {
        this.next_composite = expression;
        composite = composite.toLowerCase();
        switch (composite) {
            case "union":
                composite_function = CompositeTable.UNION;
                break;
            case "intersect":
                composite_function = CompositeTable.INTERSECT;
                break;
            case "except":
                composite_function = CompositeTable.EXCEPT;
                break;
            default:
                throw new Error("Don't understand composite function '" +
                        composite + "'");
        }
        is_composite_all = is_all;
    }


    // ---------- Implemented from StatementTreeObject ----------

    /**
     * Prepares all the expressions in the list.
     */
    private static void prepareAllInList(
            List list, ExpressionPreparer preparer) throws DatabaseException {
        for (Object o : list) {
            StatementTreeObject ob = (StatementTreeObject) o;
            ob.prepareExpressions(preparer);
        }
    }


    public void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException {
        prepareAllInList(columns, preparer);
        from_clause.prepareExpressions(preparer);
        where_clause.prepareExpressions(preparer);
        prepareAllInList(group_by, preparer);
        having_clause.prepareExpressions(preparer);

        // Go to the next chain
        if (next_composite != null) {
            next_composite.prepareExpressions(preparer);
        }
    }

    public Object clone() throws CloneNotSupportedException {
        TableSelectExpression v = (TableSelectExpression) super.clone();
        if (columns != null) {
            v.columns = (ArrayList) StatementTree.cloneSingleObject(columns);
        }
        if (from_clause != null) {
            v.from_clause = (FromClause) from_clause.clone();
        }
        if (where_clause != null) {
            v.where_clause = (SearchExpression) where_clause.clone();
        }
        if (group_by != null) {
            v.group_by = (ArrayList) StatementTree.cloneSingleObject(group_by);
        }
        if (group_max != null) {
            v.group_max = (Variable) group_max.clone();
        }
        if (having_clause != null) {
            v.having_clause = (SearchExpression) having_clause.clone();
        }
        if (next_composite != null) {
            v.next_composite = (TableSelectExpression) next_composite.clone();
        }
        return v;
    }

}
