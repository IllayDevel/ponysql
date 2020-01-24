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
 * Used in TableSet to describe how we naturally join the tables together.
 * This is used when the TableSet has evaluated the search condition and it
 * is required for any straggling tables to be naturally joined.  In SQL,
 * these joining types are specified in the FROM clause.
 * <p>
 * For example,<br><pre>
 *   FROM table_a LEFT OUTER JOIN table_b ON ( table_a.id = table_b.id ), ...
 * </pre><p>
 * A ',' should donate an INNER_JOIN in an SQL FROM clause.
 *
 * @author Tobias Downer
 */

public final class JoiningSet implements java.io.Serializable, Cloneable {

    static final long serialVersionUID = -380871062550922402L;

    /**
     * Statics for Join Types.
     */
    // Donates a standard inner join (in SQL, this is ',' in the FROM clause)
    public final static int INNER_JOIN = 1;
    // Left Outer Join,
    public final static int LEFT_OUTER_JOIN = 2;
    // Right Outer Join,
    public final static int RIGHT_OUTER_JOIN = 3;
    // Full Outer Join
    public final static int FULL_OUTER_JOIN = 4;

    /**
     * The list of tables we are joining together a JoinPart object that
     * represents how the tables are joined.
     */
    private ArrayList join_set;

    /**
     * Constructs the JoiningSet.
     */
    public JoiningSet() {
        join_set = new ArrayList();
    }

    /**
     * Resolves the schema of tables in this joining set.  This runs through
     * each table in the joining set and if the schema has not been set for the
     * table then it attempts to resolve it against the given DatabaseConnection
     * object.  This would typically be called in the preparation of a statement.
     */
    public void prepare(DatabaseConnection connection) {
    }

    /**
     * Adds a new table into the set being joined.  The table name should be the
     * unique name that distinguishes this table in the TableSet.
     */
    public void addTable(TableName table_name) {
        join_set.add(table_name);
    }

    /**
     * Hack, add a joining type to the previous entry from the end.  This is
     * an artifact of how joins are parsed.
     */
    public void addPreviousJoin(int type, Expression on_expression) {
        join_set.add(join_set.size() - 1, new JoinPart(type, on_expression));
    }

    /**
     * Adds a joining type to the set, and an 'on' expression.
     */
    public void addJoin(int type, Expression on_expression) {
        join_set.add(new JoinPart(type, on_expression));
    }

    /**
     * Adds a joining type to the set with no 'on' expression.
     */
    public void addJoin(int type) {
        join_set.add(new JoinPart(type));
    }

    /**
     * Returns the number of tables that are in this set.
     */
    public int getTableCount() {
        return (join_set.size() + 1) / 2;
    }

    /**
     * Returns the first table in the join set.
     */
    public TableName getFirstTable() {
        return getTable(0);
    }

    /**
     * Returns table 'n' in the result set where table 0 is the first table in
     * the join set.
     */
    public TableName getTable(int n) {
        return (TableName) join_set.get(n * 2);
    }

    /**
     * Sets the table at the given position in this joining set.
     */
    private void setTable(int n, TableName table) {
        join_set.set(n * 2, table);
    }

    /**
     * Returns the type of join after table 'n' in the set.  An example
     * of using this;<p><pre>
     *
     * String table1 = joins.getFirstTable();
     * for (int i = 0; i < joins.getTableCount() - 1; ++i) {
     *   int type = joins.getJoinType(i);
     *   String table2 = getTable(i + 1);
     *   // ... Join table1 and table2 ...
     *   table1 = table2;
     * }
     *
     * </pre>
     */
    public int getJoinType(int n) {
        return ((JoinPart) join_set.get((n * 2) + 1)).type;
    }

    /**
     * Returns the ON Expression for the type of join after table 'n' in the
     * set.
     */
    public Expression getOnExpression(int n) {
        return ((JoinPart) join_set.get((n * 2) + 1)).on_expression;
    }

    /**
     * Performs a deep clone on this object.
     */
    public Object clone() throws CloneNotSupportedException {
        JoiningSet v = (JoiningSet) super.clone();
        int size = join_set.size();
        ArrayList cloned_join_set = new ArrayList(size);
        v.join_set = cloned_join_set;

        for (Object element : join_set) {
            if (element instanceof TableName) {
                // immutable so leave alone
            } else if (element instanceof JoinPart) {
                element = ((JoinPart) element).clone();
            } else {
                throw new CloneNotSupportedException(element.getClass().toString());
            }
            cloned_join_set.add(element);
        }

        return v;
    }


    // ---------- Inner classes ----------

    public static class JoinPart implements java.io.Serializable, Cloneable {

        static final long serialVersionUID = -1664565759669808084L;

        /**
         * The type of join.  Either LEFT_OUTER_JOIN,
         * RIGHT_OUTER_JOIN, FULL_OUTER_JOIN, INNER_JOIN.
         */
        final int type;

        /**
         * The expression that we are joining on (eg. ON clause in SQL).  If there
         * is no ON expression (such as in the case of natural joins) then this is
         * null.
         */
        Expression on_expression;

        /**
         * Constructs the JoinPart.
         */
        public JoinPart(int type, Expression on_expression) {
            this.type = type;
            this.on_expression = on_expression;
        }

        public JoinPart(int type) {
            this(type, null);
        }

        public Object clone() throws CloneNotSupportedException {
            JoinPart v = (JoinPart) super.clone();
            if (on_expression != null) {
                v.on_expression = (Expression) on_expression.clone();
            }
            return v;
        }

    }

}
