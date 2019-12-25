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

import java.util.List;
import java.util.ArrayList;

/**
 * Various helper methods for constructing a plan tree, and the plan node
 * implementations themselves.
 *
 * @author Tobias Downer
 */

public class QueryPlan {


    /**
     * Convenience, replaces all elements of the array with clone versions of
     * themselves.
     */
    private static void cloneArray(Variable[] array)
            throws CloneNotSupportedException {
        if (array != null) {
            for (int i = 0; i < array.length; ++i) {
                array[i] = (Variable) array[i].clone();
            }
        }
    }

    /**
     * Convenience, replaces all elements of the array with clone versions of
     * themselves.
     */
    private static void cloneArray(Expression[] array)
            throws CloneNotSupportedException {
        if (array != null) {
            for (int i = 0; i < array.length; ++i) {
                array[i] = (Expression) array[i].clone();
            }
        }
    }

    private static void indentBuffer(int level, StringBuffer buf) {
        for (int i = 0; i < level; ++i) {
            buf.append(' ');
        }
    }


    // ---------- Plan node implementations ----------

    /**
     * A QueryPlanNode with a single child.
     */
    public static abstract class SingleQueryPlanNode implements QueryPlanNode {

        static final long serialVersionUID = -6753991881140638658L;

        /**
         * The single child node.
         */
        protected QueryPlanNode child;

        /**
         * Constructor.
         */
        protected SingleQueryPlanNode(QueryPlanNode child) {
            this.child = child;
        }

        /**
         * Returns the child plan.
         */
        public QueryPlanNode child() {
            return child;
        }

        /**
         * Default implementation delegates responsibility to child.
         */
        public ArrayList discoverTableNames(ArrayList list) {
            return child.discoverTableNames(list);
        }

        /**
         * Default implementation that discovers correlated variables for the
         * given offset level.
         */
        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return child.discoverCorrelatedVariables(level, list);
        }

        /**
         * Deep clone.
         */
        public Object clone() throws CloneNotSupportedException {
            SingleQueryPlanNode node = (SingleQueryPlanNode) super.clone();
            node.child = (QueryPlanNode) child.clone();
            return node;
        }

        public String titleString() {
            return getClass().getName();
        }

        public void debugString(int level, StringBuffer buf) {
            indentBuffer(level, buf);
            buf.append(titleString());
            buf.append('\n');
            child.debugString(level + 2, buf);
        }

    }

    /**
     * A QueryPlanNode that is a branch with two child nodes.
     */
    public static abstract class BranchQueryPlanNode implements QueryPlanNode {

        static final long serialVersionUID = 2938130775577221138L;

        /**
         * The left and right node.
         */
        protected QueryPlanNode left, right;

        /**
         * The Constructor.
         */
        protected BranchQueryPlanNode(QueryPlanNode left, QueryPlanNode right) {
            this.left = left;
            this.right = right;
        }

        /**
         * Returns the left node.
         */
        public QueryPlanNode left() {
            return left;
        }

        /**
         * Returns the right node.
         */
        public QueryPlanNode right() {
            return right;
        }

        /**
         * Default implementation delegates responsibility to children.
         */
        public ArrayList discoverTableNames(ArrayList list) {
            return right.discoverTableNames(
                    left.discoverTableNames(list));
        }

        /**
         * Default implementation that discovers correlated variables for the
         * given offset level.
         */
        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return right.discoverCorrelatedVariables(level,
                    left.discoverCorrelatedVariables(level, list));
        }

        /**
         * Deep clone.
         */
        public Object clone() throws CloneNotSupportedException {
            BranchQueryPlanNode node = (BranchQueryPlanNode) super.clone();
            node.left = (QueryPlanNode) left.clone();
            node.right = (QueryPlanNode) right.clone();
            return node;
        }

        public String titleString() {
            return getClass().getName();
        }

        public void debugString(int level, StringBuffer buf) {
            indentBuffer(level, buf);
            buf.append(titleString());
            buf.append('\n');
            left.debugString(level + 2, buf);
            right.debugString(level + 2, buf);
        }

    }


    /**
     * The node for fetching a table from the current transaction.  This is
     * a tree node and has no children.
     */
    public static class FetchTableNode implements QueryPlanNode {

        static final long serialVersionUID = 7545493568015241717L;

        /**
         * The name of the table to fetch.
         */
        private TableName table_name;

        /**
         * The name to alias the table as.
         */
        private TableName alias_name;

        public FetchTableNode(TableName table_name, TableName aliased_as) {
            this.table_name = table_name;
            this.alias_name = aliased_as;
        }

        /**
         * Adds the table name to the list if it's not already in there.
         */
        public ArrayList discoverTableNames(ArrayList list) {
            if (!list.contains(table_name)) {
                list.add(table_name);
            }
            return list;
        }

        public Table evaluate(QueryContext context) {
            // MILD HACK: Cast the context to a DatabaseQueryContext
            DatabaseQueryContext db_context = (DatabaseQueryContext) context;
            DataTable t = db_context.getTable(table_name);
            if (alias_name != null) {
                return new ReferenceTable(t, alias_name);
            }
            return t;
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return list;
        }

        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public String titleString() {
            return "FETCH: " + table_name + " AS " + alias_name;
        }

        public void debugString(int level, StringBuffer buf) {
            indentBuffer(level, buf);
            buf.append(titleString());
            buf.append('\n');
        }

    }

    /**
     * A node for creating a table with a single row.  This table is useful for
     * queries that have no underlying row.  For example, a pure functional
     * table expression.
     */
    public static class SingleRowTableNode implements QueryPlanNode {

        static final long serialVersionUID = -7180494964138911604L;

        public SingleRowTableNode() {
        }

        public ArrayList discoverTableNames(ArrayList list) {
            return list;
        }

        public Table evaluate(QueryContext context) {
            // MILD HACK: Cast the context to a DatabaseQueryContext
            DatabaseQueryContext db_context = (DatabaseQueryContext) context;
            return db_context.getDatabase().getSingleRowTable();
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return list;
        }

        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public String titleString() {
            return "SINGLE ROW";
        }

        public void debugString(int level, StringBuffer buf) {
            indentBuffer(level, buf);
            buf.append(titleString());
            buf.append('\n');
        }

    }

    /**
     * The node that fetches a view from the current connection.  This is a
     * tree node that has no children, however the child can be created by
     * calling the 'createViewChildNode' method.  This node can be removed from a
     * plan tree by calling the 'createViewChildNode' method and substituting this
     * node with the returned child.  For a planner that normalizes and optimizes
     * plan trees, this is a useful feature.
     */
    public static class FetchViewNode implements QueryPlanNode {

        static final long serialVersionUID = -6557333346211179284L;

        /**
         * The name of the view to fetch.
         */
        private TableName table_name;

        /**
         * The name to alias the table as.
         */
        private TableName alias_name;

        public FetchViewNode(TableName table_name, TableName aliased_as) {
            this.table_name = table_name;
            this.alias_name = aliased_as;
        }

        /**
         * Returns the QueryPlanNode that resolves to the view.  This looks up the
         * query plan in the context given.
         */
        public QueryPlanNode createViewChildNode(QueryContext context) {
            DatabaseQueryContext db = (DatabaseQueryContext) context;
            return db.createViewQueryPlanNode(table_name);
        }

        /**
         * Adds the table name to the list if it's not already in there.
         */
        public ArrayList discoverTableNames(ArrayList list) {
            if (!list.contains(table_name)) {
                list.add(table_name);
            }
            return list;
        }

        public Table evaluate(QueryContext context) {

            // Create the view child node
            QueryPlanNode node = createViewChildNode(context);
            // Evaluate the plan
            Table t = node.evaluate(context);

            if (alias_name != null) {
                return new ReferenceTable(t, alias_name);
            } else {
                return t;
            }

        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return list;
        }

        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public String titleString() {
            return "VIEW: " + table_name + " AS " + alias_name;
        }

        public void debugString(int level, StringBuffer buf) {
            indentBuffer(level, buf);
            buf.append(titleString());
            buf.append('\n');
        }

    }


    /**
     * The node for performing a simple indexed query on a single column of the
     * child node.  Finds the set from the child node that matches the range.
     * <p>
     * The given Expression object must conform to a number of rules.  It may
     * reference only one column in the child node.  It must consist of only
     * simple mathemetical and logical operators (<, >, =, <>, >=, <=, AND, OR).
     * The left side of each mathematical operator must be a variable, and the
     * right side must be a constant (parameter subsitution or correlated value).
     * For example;
     *   (col > 10 AND col < 100) OR col > 1000 OR col == 10
     * <p>
     * Breaking any of these rules will mean the range select can not happen.
     */
    public static class RangeSelectNode extends SingleQueryPlanNode {

        static final long serialVersionUID = -108747827391465748L;

        /**
         * A simple expression that represents the range to select.  See the
         * class comments for a description for how this expression must be
         * formed.
         */
        private Expression expression;

        public RangeSelectNode(QueryPlanNode child, Expression exp) {
            super(child);
            this.expression = exp;
        }

        /**
         * Given an Expression, this will return a list of expressions that can be
         * safely executed as a set of 'and' operations.  For example, an
         * expression of 'a=9 and b=c and d=2' would return the list; 'a=9','b=c',
         * 'd=2'.
         * <p>
         * If non 'and' operators are found then the reduction stops.
         */
        private ArrayList createAndList(ArrayList list, Expression exp) {
            return exp.breakByOperator(list, "and");
        }

        /**
         * Updates a range with the given expression.
         */
        private void updateRange(QueryContext context, SelectableRangeSet range,
                                 DataTableColumnDef field, Expression e) {
            Operator op = (Operator) e.last();
            Expression[] exps = e.split();
            // Evaluate to an object
            TObject cell = exps[1].evaluate(null, null, context);

            // If the evaluated object is not of a comparable type, then it becomes
            // null.
            TType field_type = field.getTType();
            if (!cell.getTType().comparableTypes(field_type)) {
                cell = new TObject(field_type, null);
            }

            // Intersect this in the range set
            range.intersect(op, cell);
        }

        /**
         * Calculates a list of SelectableRange objects that represent the range
         * of the expression.
         */
        private void calcRange(final QueryContext context,
                               final DataTableColumnDef field,
                               final SelectableRangeSet range,
                               final Expression exp) {
            Operator op = (Operator) exp.last();
            if (op.isLogical()) {
                if (op.is("and")) {
                    ArrayList and_list = createAndList(new ArrayList(), exp);
                    int sz = and_list.size();
                    for (int i = 0; i < sz; ++i) {
                        updateRange(context, range, field, (Expression) and_list.get(i));
                    }
                } else if (op.is("or")) {
                    // Split left and right of logical operator.
                    Expression[] exps = exp.split();
                    // Calculate the range of the left and right
                    SelectableRangeSet left = new SelectableRangeSet();
                    calcRange(context, field, left, exps[0]);
                    SelectableRangeSet right = new SelectableRangeSet();
                    calcRange(context, field, right, exps[1]);

                    // Union the left and right range with the current range
                    range.union(left);
                    range.union(right);
                } else {
                    throw new Error("Unrecognised logical operator.");
                }
            } else {
                // Not an operator so this is the value.
                updateRange(context, range, field, exp);
            }

        }


        public Table evaluate(QueryContext context) {
            Table t = child.evaluate(context);

            Expression exp = expression;

            // Assert that all variables in the expression are identical.
            List all_vars = exp.allVariables();
            Variable v = null;
            int sz = all_vars.size();
            for (int i = 0; i < sz; ++i) {
                Variable cv = (Variable) all_vars.get(i);
                if (v != null) {
                    if (!cv.equals(v)) {
                        throw new Error("Assertion failed: " +
                                "Range plan does not contain common variable.");
                    }
                }
                v = cv;
            }

            // Find the variable field in the table.
            int col = t.findFieldName(v);
            if (col == -1) {
                throw new Error("Couldn't find column reference in table: " + v);
            }
            DataTableColumnDef field = t.getColumnDefAt(col);
            // Calculate the range
            SelectableRangeSet range = new SelectableRangeSet();
            calcRange(context, field, range, exp);

//      System.out.println("RANGE: ");
//      System.out.println(range);

            // Select the range from the table
            SelectableRange[] ranges = range.toSelectableRangeArray();
            return t.rangeSelect(v, ranges);

        }

        public ArrayList discoverTableNames(ArrayList list) {
            return expression.discoverTableNames(super.discoverTableNames(list));
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
//      System.out.println(expression);
            return expression.discoverCorrelatedVariables(level,
                    super.discoverCorrelatedVariables(level, list));
        }

        public Object clone() throws CloneNotSupportedException {
            RangeSelectNode node = (RangeSelectNode) super.clone();
            node.expression = (Expression) expression.clone();
            return node;
        }

        public String titleString() {
            return "RANGE: " + expression;
        }

    }

    /**
     * The node for performing a simple select operation on a table.  The simple
     * select requires a LHS variable, an operator, and an expression
     * representing the RHS.
     */
    public static class SimpleSelectNode extends SingleQueryPlanNode {

        static final long serialVersionUID = 5502157970886270867L;

        /**
         * The LHS variable.
         */
        private Variable left_var;

        /**
         * The operator to select under (=, <>, >, <, >=, <=).
         */
        private Operator op;

        /**
         * The RHS expression.
         */
        private Expression right_expression;

        public SimpleSelectNode(QueryPlanNode child,
                                Variable left_var, Operator op,
                                Expression right_expression) {
            super(child);
            this.left_var = left_var;
            this.op = op;
            this.right_expression = right_expression;
        }

        public Table evaluate(QueryContext context) {
            // Solve the child branch result
            Table table = child.evaluate(context);

            // The select operation.
            return table.simpleSelect(context,
                    left_var, op, right_expression);
        }

        public ArrayList discoverTableNames(ArrayList list) {
            return right_expression.discoverTableNames(
                    super.discoverTableNames(list));
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return right_expression.discoverCorrelatedVariables(level,
                    super.discoverCorrelatedVariables(level, list));
        }

        public Object clone() throws CloneNotSupportedException {
            SimpleSelectNode node = (SimpleSelectNode) super.clone();
            node.left_var = (Variable) left_var.clone();
            node.right_expression = (Expression) right_expression.clone();
            return node;
        }

        public String titleString() {
            return "SIMPLE: " + left_var + op + right_expression;
        }

    }

    /**
     * The node for performing an equi-select on a group of columns of the
     * child node.  This is a separate node instead of chained
     * IndexedSelectNode's so that we might exploit multi-column indexes.
     */
    public static class MultiColumnEquiSelectNode extends SingleQueryPlanNode {

        static final long serialVersionUID = -1407710412096857588L;

        /**
         * The list of columns to select the range of.
         */
        private Variable[] columns;

        /**
         * The values of the cells to equi-select (must be constant expressions).
         */
        private Expression[] values;

        public MultiColumnEquiSelectNode(QueryPlanNode child,
                                         Variable[] columns, Expression[] values) {
            super(child);
            this.columns = columns;
            this.values = values;
        }

        public Table evaluate(QueryContext context) {
            Table t = child.evaluate(context);

            // PENDING: Exploit multi-column indexes when they are implemented...

            // We select each column in turn
            Operator EQUALS_OP = Operator.get("=");
            for (int i = 0; i < columns.length; ++i) {
                t = t.simpleSelect(context, columns[i], EQUALS_OP, values[i]);
            }

            return t;
        }

        public ArrayList discoverTableNames(ArrayList list) {
            throw new Error("PENDING");
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            throw new Error("PENDING");
        }

        public Object clone() throws CloneNotSupportedException {
            MultiColumnEquiSelectNode node =
                    (MultiColumnEquiSelectNode) super.clone();
            cloneArray(node.columns);
            cloneArray(node.values);
            return node;
        }

    }

    /**
     * The node for performing a functional select operation on the child node.
     * Some examples of this type of query are;
     *   CONCAT(a, ' ', b) > 'abba boh'
     *   TONUMBER(DATEFORMAT(a, 'yyyy')) > 2001
     *   LOWER(a) < 'ook'
     * The reason this is a separate node is because it is possible to exploit
     * a functional indexes on a table with this node.
     * <p>
     * The given expression MUST be of the form;
     *   'function_expression' 'operator' 'constant'
     */
    public static class FunctionalSelectNode extends SingleQueryPlanNode {

        static final long serialVersionUID = -1428022600352236457L;

        /**
         * The function expression (eg. CONCAT(a, ' ', b) == 'abba bo').
         */
        private Expression expression;

        public FunctionalSelectNode(QueryPlanNode child, Expression exp) {
            super(child);
            this.expression = exp;
        }

        public Table evaluate(QueryContext context) {
            Table t = child.evaluate(context);
            // NOTE: currently this uses exhaustive select but should exploit
            //   function indexes when they are available.
            return t.exhaustiveSelect(context, expression);
        }

        public ArrayList discoverTableNames(ArrayList list) {
            return expression.discoverTableNames(super.discoverTableNames(list));
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return expression.discoverCorrelatedVariables(level,
                    super.discoverCorrelatedVariables(level, list));
        }

        public Object clone() throws CloneNotSupportedException {
            FunctionalSelectNode node = (FunctionalSelectNode) super.clone();
            node.expression = (Expression) expression.clone();
            return node;
        }

    }

    /**
     * The node for performing a exhaustive select operation on the child node.
     * This node will iterate through the entire child result and all
     * results that evaulate to true are included in the result.
     * <p>
     * NOTE: The Expression may have correlated sub-queries.
     */
    public static class ExhaustiveSelectNode extends SingleQueryPlanNode {

        static final long serialVersionUID = -2005551680157574172L;

        /**
         * The search expression.
         */
        private Expression expression;

        public ExhaustiveSelectNode(QueryPlanNode child, Expression exp) {
            super(child);
            this.expression = exp;
        }

        public Table evaluate(QueryContext context) {
            Table t = child.evaluate(context);
            return t.exhaustiveSelect(context, expression);
        }

        public ArrayList discoverTableNames(ArrayList list) {
            return expression.discoverTableNames(super.discoverTableNames(list));
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return expression.discoverCorrelatedVariables(level,
                    super.discoverCorrelatedVariables(level, list));
        }

        public Object clone() throws CloneNotSupportedException {
            ExhaustiveSelectNode node = (ExhaustiveSelectNode) super.clone();
            node.expression = (Expression) expression.clone();
            return node;
        }

        public String titleString() {
            return "EXHAUSTIVE: " + expression;
        }

    }

    /**
     * The node for evaluating an expression that contains entirely constant
     * values (no variables).
     */
    public static class ConstantSelectNode extends SingleQueryPlanNode {

        static final long serialVersionUID = -4435336817396073146L;

        /**
         * The search expression.
         */
        private Expression expression;

        public ConstantSelectNode(QueryPlanNode child, Expression exp) {
            super(child);
            this.expression = exp;
        }

        public Table evaluate(QueryContext context) {
            // Evaluate the expression
            TObject v = expression.evaluate(null, null, context);
            // If it evaluates to NULL or FALSE then return an empty set
            if (v.isNull() || v.getObject().equals(Boolean.FALSE)) {
                return child.evaluate(context).emptySelect();
            } else {
                return child.evaluate(context);
            }
        }

        public ArrayList discoverTableNames(ArrayList list) {
            return expression.discoverTableNames(super.discoverTableNames(list));
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return expression.discoverCorrelatedVariables(level,
                    super.discoverCorrelatedVariables(level, list));
        }

        public Object clone() throws CloneNotSupportedException {
            ConstantSelectNode node = (ConstantSelectNode) super.clone();
            node.expression = (Expression) expression.clone();
            return node;
        }

        public String titleString() {
            return "CONSTANT: " + expression;
        }

    }

    /**
     * The node for evaluating a simple pattern search on a table which
     * includes a single left hand variable or constant, a pattern type (LIKE,
     * NOT LIKE or REGEXP), and a right hand constant (eg. 'T__y').  If the
     * expression is not in this form then this node will not operate
     * correctly.
     */
    public static class SimplePatternSelectNode extends SingleQueryPlanNode {

        static final long serialVersionUID = -8247282157310682761L;

        /**
         * The search expression.
         */
        private Expression expression;

        public SimplePatternSelectNode(QueryPlanNode child, Expression exp) {
            super(child);
            this.expression = exp;
        }

        public Table evaluate(QueryContext context) {
            // Evaluate the child
            Table t = child.evaluate(context);
            // Perform the pattern search expression on the table.
            // Split the expression,
            Expression[] exps = expression.split();
            Variable lhs_var = exps[0].getVariable();
            if (lhs_var != null) {
                // LHS is a simple variable so do a simple select
                Operator op = (Operator) expression.last();
                return t.simpleSelect(context, lhs_var, op, exps[1]);
            } else {
                // LHS must be a constant so we can just evaluate the expression
                // and see if we get true, false, null, etc.
                TObject v = expression.evaluate(null, context);
                // If it evaluates to NULL or FALSE then return an empty set
                if (v.isNull() || v.getObject().equals(Boolean.FALSE)) {
                    return t.emptySelect();
                } else {
                    return t;
                }
            }
        }

        public ArrayList discoverTableNames(ArrayList list) {
            return expression.discoverTableNames(super.discoverTableNames(list));
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return expression.discoverCorrelatedVariables(level,
                    super.discoverCorrelatedVariables(level, list));
        }

        public Object clone() throws CloneNotSupportedException {
            SimplePatternSelectNode node = (SimplePatternSelectNode) super.clone();
            node.expression = (Expression) expression.clone();
            return node;
        }

        public String titleString() {
            return "PATTERN: " + expression;
        }

    }

    /**
     * The node for finding a subset and renaming the columns of the results in
     * the child node.
     */
    public static class SubsetNode extends SingleQueryPlanNode {

        static final long serialVersionUID = 3784462788248510832L;

        /**
         * The original columns in the child that we are to make the subset of.
         */
        private Variable[] original_columns;

        /**
         * New names to assign the columns.
         */
        private Variable[] new_column_names;


        public SubsetNode(QueryPlanNode child,
                          Variable[] original_columns,
                          Variable[] new_column_names) {
            super(child);
            this.original_columns = original_columns;
            this.new_column_names = new_column_names;

        }

        public Table evaluate(QueryContext context) {
            Table t = child.evaluate(context);

            int sz = original_columns.length;
            int[] col_map = new int[sz];

//      // DEBUG
//      for (int n = 0; n < t.getColumnCount(); ++n) {
//        System.out.print(t.getResolvedVariable(n).toTechString());
//        System.out.print(", ");
//      }
//      System.out.println();
//      // - DEBUG

            for (int i = 0; i < sz; ++i) {

//        // DEBUG
//        System.out.print(t.getClass() + ".findFieldName(" +
//                         original_columns[i].toTechString() + ") = ");
//        // - DEBUG

                col_map[i] = t.findFieldName(original_columns[i]);

//        // DEBUG
//        System.out.println(col_map[i]);
//        // - DEBUG

            }

            SubsetColumnTable col_table = new SubsetColumnTable(t);
            col_table.setColumnMap(col_map, new_column_names);

            return col_table;
        }

        // ---------- Set methods ----------

        /**
         * Sets the given table name of the resultant table.  This is intended
         * if we want to create a sub-query that has an aliased table name.
         */
        public void setGivenName(TableName name) {
//      given_name = name;
            if (name != null) {
                int sz = new_column_names.length;
                for (int i = 0; i < sz; ++i) {
                    new_column_names[i].setTableName(name);
                }
            }
        }

        // ---------- Get methods ----------

        /**
         * Returns the list of original columns that represent the mappings from
         * the columns in this subset.
         */
        public Variable[] getOriginalColumns() {
            return original_columns;
        }

        /**
         * Returns the list of new column names that represent the new columns
         * in this subset.
         */
        public Variable[] getNewColumnNames() {
            return new_column_names;
        }

        public Object clone() throws CloneNotSupportedException {
            SubsetNode node = (SubsetNode) super.clone();
            cloneArray(node.original_columns);
            cloneArray(node.new_column_names);
            return node;
        }

        public String titleString() {
            StringBuffer buf = new StringBuffer();
            buf.append("SUBSET: ");
            for (int i = 0; i < new_column_names.length; ++i) {
                buf.append(new_column_names[i]);
                buf.append("->");
                buf.append(original_columns[i]);
                buf.append(", ");
            }
            return new String(buf);
        }

    }

    /**
     * The node for performing a distinct operation on the given columns of the
     * child node.
     */
    public static class DistinctNode extends SingleQueryPlanNode {

        static final long serialVersionUID = -1538264313804102373L;

        /**
         * The list of columns to be distinct.
         */
        private Variable[] columns;

        public DistinctNode(QueryPlanNode child, Variable[] columns) {
            super(child);
            this.columns = columns;
        }

        public Table evaluate(QueryContext context) {
            Table t = child.evaluate(context);
            int sz = columns.length;
            int[] col_map = new int[sz];
            for (int i = 0; i < sz; ++i) {
                col_map[i] = t.findFieldName(columns[i]);
            }
            return t.distinct(col_map);
        }

        public Object clone() throws CloneNotSupportedException {
            DistinctNode node = (DistinctNode) super.clone();
            cloneArray(node.columns);
            return node;
        }

        public String titleString() {
            StringBuffer buf = new StringBuffer();
            buf.append("DISTINCT: (");
            for (int i = 0; i < columns.length; ++i) {
                buf.append(columns[i]);
                buf.append(", ");
            }
            buf.append(")");
            return new String(buf);
        }

    }

    /**
     * The node for performing a sort operation on the given columns of the
     * child node.
     */
    public static class SortNode extends SingleQueryPlanNode {

        static final long serialVersionUID = 3644480534542996928L;

        /**
         * The list of columns to sort.
         */
        private Variable[] columns;

        /**
         * Whether to sort the column in ascending or descending order
         */
        private boolean[] correct_ascending;

        public SortNode(QueryPlanNode child, Variable[] columns,
                        boolean[] ascending) {
            super(child);
            this.columns = columns;
            this.correct_ascending = ascending;

            // How we handle ascending/descending order
            // ----------------------------------------
            // Internally to the database, all columns are naturally ordered in
            // ascending order (start at lowest and end on highest).  When a column
            // is ordered in descending order, a fast way to achieve this is to take
            // the ascending set and reverse it.  This works for single columns,
            // however some thought is required for handling multiple column.  We
            // order columns from RHS to LHS.  If LHS is descending then this will
            // order the RHS incorrectly if we leave as is.  Therefore, we must do
            // some pre-processing that looks ahead on any descending orders and
            // reverses the order of the columns to the right.  This pre-processing
            // is done in the first pass.

            int sz = ascending.length;
            for (int n = 0; n < sz - 1; ++n) {
                if (!ascending[n]) {    // if descending...
                    // Reverse order of all columns to the right...
                    for (int p = n + 1; p < sz; ++p) {
                        ascending[p] = !ascending[p];
                    }
                }
            }

        }

        public Table evaluate(QueryContext context) {
            Table t = child.evaluate(context);
            // Sort the results by the columns in reverse-safe order.
            int sz = correct_ascending.length;
            for (int n = sz - 1; n >= 0; --n) {
                t = t.orderByColumn(columns[n], correct_ascending[n]);
            }
            return t;
        }

        public Object clone() throws CloneNotSupportedException {
            SortNode node = (SortNode) super.clone();
            cloneArray(node.columns);
            return node;
        }

        public String titleString() {
            StringBuffer buf = new StringBuffer();
            buf.append("SORT: (");
            for (int i = 0; i < columns.length; ++i) {
                buf.append(columns[i]);
                if (correct_ascending[i]) {
                    buf.append(" ASC");
                } else {
                    buf.append(" DESC");
                }
                buf.append(", ");
            }
            buf.append(")");
            return new String(buf);
        }

    }

    /**
     * The node for performing a grouping operation on the columns of the child
     * node.  As well as grouping, any aggregate functions must also be defined
     * with this plan.
     * <p>
     * NOTE: The whole child is a group if columns is null.
     */
    public static class GroupNode extends SingleQueryPlanNode {

        static final long serialVersionUID = 7140928678192396348L;

        /**
         * The columns to group by.
         */
        private Variable[] columns;

        /**
         * The group max column.
         */
        private Variable group_max_column;

        /**
         * Any aggregate functions (or regular function columns) that are to be
         * planned.
         */
        private Expression[] function_list;

        /**
         * The list of names to give each function table.
         */
        private String[] name_list;


        /**
         * Groups over the given columns from the child.
         */
        public GroupNode(QueryPlanNode child, Variable[] columns,
                         Variable group_max_column,
                         Expression[] function_list, String[] name_list) {
            super(child);
            this.columns = columns;
            this.group_max_column = group_max_column;
            this.function_list = function_list;
            this.name_list = name_list;
        }

        /**
         * Groups over the entire child (always ends in 1 result in set).
         */
        public GroupNode(QueryPlanNode child, Variable group_max_column,
                         Expression[] function_list, String[] name_list) {
            this(child, null, group_max_column, function_list, name_list);
        }

        public Table evaluate(QueryContext context) {
            Table child_table = child.evaluate(context);
            DatabaseQueryContext db_context = (DatabaseQueryContext) context;
            FunctionTable fun_table =
                    new FunctionTable(child_table, function_list, name_list, db_context);
            // If no columns then it is implied the whole table is the group.
            if (columns == null) {
                fun_table.setWholeTableAsGroup();
            } else {
                fun_table.createGroupMatrix(columns);
            }
            return fun_table.mergeWithReference(group_max_column);
        }

        public ArrayList discoverTableNames(ArrayList list) {
            list = super.discoverTableNames(list);
            for (int i = 0; i < function_list.length; ++i) {
                list = function_list[i].discoverTableNames(list);
            }
            return list;
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            list = super.discoverCorrelatedVariables(level, list);
            for (int i = 0; i < function_list.length; ++i) {
                list = function_list[i].discoverCorrelatedVariables(level, list);
            }
            return list;
        }

        public Object clone() throws CloneNotSupportedException {
            GroupNode node = (GroupNode) super.clone();
            cloneArray(node.columns);
            cloneArray(node.function_list);
            if (group_max_column != null) {
                node.group_max_column = (Variable) group_max_column.clone();
            } else {
                node.group_max_column = null;
            }
            return node;
        }

        public String titleString() {
            StringBuffer buf = new StringBuffer();
            buf.append("GROUP: (");
            if (columns == null) {
                buf.append("WHOLE TABLE");
            } else {
                for (int i = 0; i < columns.length; ++i) {
                    buf.append(columns[i]);
                    buf.append(", ");
                }
            }
            buf.append(")");
            if (function_list != null) {
                buf.append(" FUNS: [");
                for (int i = 0; i < function_list.length; ++i) {
                    buf.append(function_list[i]);
                    buf.append(", ");
                }
                buf.append("]");
            }
            return new String(buf);
        }

    }

    /**
     * The node for merging the child node with a set of new function columns
     * over the entire result.  For example, we may want to add an expression
     * 'a + 10' or 'coalesce(a, b, 1)'.
     */
    public static class CreateFunctionsNode extends SingleQueryPlanNode {

        static final long serialVersionUID = -181012844247626327L;

        /**
         * The list of functions to create.
         */
        private Expression[] function_list;

        /**
         * The list of names to give each function table.
         */
        private String[] name_list;

        /**
         * Constructor.
         */
        public CreateFunctionsNode(QueryPlanNode child, Expression[] function_list,
                                   String[] name_list) {
            super(child);
            this.function_list = function_list;
            this.name_list = name_list;
        }

        public Table evaluate(QueryContext context) {
            Table child_table = child.evaluate(context);
            DatabaseQueryContext db_context = (DatabaseQueryContext) context;
            FunctionTable fun_table =
                    new FunctionTable(child_table, function_list, name_list, db_context);
            Table t = fun_table.mergeWithReference(null);
            return t;
        }

        public ArrayList discoverTableNames(ArrayList list) {
            list = super.discoverTableNames(list);
            for (int i = 0; i < function_list.length; ++i) {
                list = function_list[i].discoverTableNames(list);
            }
            return list;
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            list = super.discoverCorrelatedVariables(level, list);
            for (int i = 0; i < function_list.length; ++i) {
                list = function_list[i].discoverCorrelatedVariables(level, list);
            }
            return list;
        }

        public Object clone() throws CloneNotSupportedException {
            CreateFunctionsNode node = (CreateFunctionsNode) super.clone();
            cloneArray(node.function_list);
            return node;
        }

        public String titleString() {
            StringBuffer buf = new StringBuffer();
            buf.append("FUNCTIONS: (");
            for (int i = 0; i < function_list.length; ++i) {
                buf.append(function_list[i]);
                buf.append(", ");
            }
            buf.append(")");
            return new String(buf);
        }

    }

    /**
     * A marker node that takes the result of a child and marks it as a name
     * that can later be retrieved.  This is useful for implementing things
     * such as outer joins.
     */
    public static class MarkerNode extends SingleQueryPlanNode {

        static final long serialVersionUID = -8321710589608765270L;

        /**
         * The name of this mark.
         */
        private String mark_name;

        /**
         * Constructor.
         */
        public MarkerNode(QueryPlanNode child, String mark_name) {
            super(child);
            this.mark_name = mark_name;
        }

        public Table evaluate(QueryContext context) {
            Table child_table = child.evaluate(context);
            context.addMarkedTable(mark_name, child_table);
            return child_table;
        }

        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public String titleString() {
            return "MARKER: " + mark_name;
        }

    }

    /**
     * A cache point node that only evaluates the child if the result can not
     * be found in the cache with the given unique id.
     */
    public static class CachePointNode extends SingleQueryPlanNode {

        static final long serialVersionUID = 7866310557831478639L;

        /**
         * The unique identifier of this cache point.
         */
        private long id;

        private final static Object GLOB_LOCK = new Object();
        private static int GLOB_ID = 0;

        /**
         * Constructor.
         */
        public CachePointNode(QueryPlanNode child) {
            super(child);
            synchronized (GLOB_LOCK) {
                id = (System.currentTimeMillis() << 16) | (GLOB_ID & 0x0FFFF);
                ++GLOB_ID;
            }
        }

        public Table evaluate(QueryContext context) {
            // Is the result available in the context?
            Table child_table = context.getCachedNode(id);
            if (child_table == null) {
                // No so evaluate the child and cache it
                child_table = child.evaluate(context);
                context.putCachedNode(id, child_table);
            }
            return child_table;
        }

        public Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public String titleString() {
            return "CACHE: " + id;
        }

    }


    /**
     * A branch node for naturally joining two tables together.  These branches
     * should be optimized out if possible because they result in huge results.
     */
    public static class NaturalJoinNode extends BranchQueryPlanNode {

        static final long serialVersionUID = 942526205653132810L;

        public NaturalJoinNode(QueryPlanNode left, QueryPlanNode right) {
            super(left, right);
        }

        public Table evaluate(QueryContext context) {
            // Solve the left branch result
            Table left_result = left.evaluate(context);
            // Solve the Join (natural)
            return left_result.join(right.evaluate(context));
        }

        public String titleString() {
            return "NATURAL JOIN";
        }

    }

    /**
     * A branch node for equi-joining two tables together given two sets of
     * columns.  This is a seperate node from a general join operation to allow
     * for optimizations with multi-column indexes.
     * <p>
     * An equi-join is the most common type of join.
     * <p>
     * At query runtime, this decides the best best way to perform the join,
     * either by
     */
    public static class EquiJoinNode extends BranchQueryPlanNode {

        static final long serialVersionUID = 113332589582049607L;

        /**
         * The columns in the left table.
         */
        private Variable[] left_columns;

        /**
         * The columns in the right table.
         */
        private Variable[] right_columns;

        public EquiJoinNode(QueryPlanNode left, QueryPlanNode right,
                            Variable[] left_cols, Variable[] right_cols) {
            super(left, right);
            this.left_columns = left_cols;
            this.right_columns = right_cols;
        }

        public Table evaluate(QueryContext context) {
            // Solve the left branch result
            Table left_result = left.evaluate(context);
            // Solve the right branch result
            Table right_result = right.evaluate(context);

            // PENDING: This needs to migrate to a better implementation that
            //   exploits multi-column indexes if one is defined that can be used.

            Variable first_left = left_columns[0];
            Variable first_right = right_columns[0];

            Operator EQUALS_OP = Operator.get("=");

            Table result = left_result.simpleJoin(context, right_result,
                    first_left, EQUALS_OP, new Expression(first_right));

            int sz = left_columns.length;
            // If there are columns left to equi-join, we resolve the rest with a
            // single exhaustive select of the form,
            //   ( table1.col2 = table2.col2 AND table1.col3 = table2.col3 AND ... )
            if (sz > 1) {
                // Form the expression
                Expression rest_expression = new Expression();
                for (int i = 1; i < sz; ++i) {
                    Variable left_var = left_columns[i];
                    Variable right_var = right_columns[i];
                    rest_expression.addElement(left_var);
                    rest_expression.addElement(right_var);
                    rest_expression.addOperator(EQUALS_OP);
                }
                Operator AND_OP = Operator.get("and");
                for (int i = 2; i < sz; ++i) {
                    rest_expression.addOperator(AND_OP);
                }
                result = result.exhaustiveSelect(context, rest_expression);
            }

            return result;
        }

        public Object clone() throws CloneNotSupportedException {
            EquiJoinNode node = (EquiJoinNode) super.clone();
            cloneArray(node.left_columns);
            cloneArray(node.right_columns);
            return node;
        }

    }

    /**
     * A branch node for a non-equi join between two tables.
     * <p>
     * NOTE: The cost of a LeftJoin is higher if the right child result is
     *   greater than the left child result.  The plan should be arranged so
     *   smaller results are on the left.
     */
    public static class JoinNode extends BranchQueryPlanNode {

        static final long serialVersionUID = 4133205808616807832L;

        /**
         * The variable in the left table to be joined.
         */
        private Variable left_var;

        /**
         * The operator to join under (=, <>, >, <, >=, <=).
         */
        private Operator join_op;

        /**
         * The expression evaluated on the right table.
         */
        private Expression right_expression;

        public JoinNode(QueryPlanNode left, QueryPlanNode right,
                        Variable left_var, Operator join_op,
                        Expression right_expression) {
            super(left, right);
            this.left_var = left_var;
            this.join_op = join_op;
            this.right_expression = right_expression;
        }

        public Table evaluate(QueryContext context) {
            // Solve the left branch result
            Table left_result = left.evaluate(context);
            // Solve the right branch result
            Table right_result = right.evaluate(context);

            // If the right_expression is a simple variable then we have the option
            // of optimizing this join by putting the smallest table on the LHS.
            Variable rhs_var = right_expression.getVariable();
            Variable lhs_var = left_var;
            Operator op = join_op;
            if (rhs_var != null) {
                // We should arrange the expression so the right table is the smallest
                // of the sides.
                // If the left result is less than the right result
                if (left_result.getRowCount() < right_result.getRowCount()) {
                    // Reverse the join
                    right_expression = new Expression(lhs_var);
                    lhs_var = rhs_var;
                    op = op.reverse();
                    // Reverse the tables.
                    Table t = right_result;
                    right_result = left_result;
                    left_result = t;
                }
            }

            // The join operation.
            return left_result.simpleJoin(context, right_result,
                    lhs_var, op, right_expression);
        }

        public ArrayList discoverTableNames(ArrayList list) {
            return right_expression.discoverTableNames(
                    super.discoverTableNames(list));
        }

        public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
            return right_expression.discoverCorrelatedVariables(level,
                    super.discoverCorrelatedVariables(level, list));
        }

        public Object clone() throws CloneNotSupportedException {
            JoinNode node = (JoinNode) super.clone();
            node.left_var = (Variable) left_var.clone();
            node.right_expression = (Expression) right_expression.clone();
            return node;
        }

        public String titleString() {
            return "JOIN: " + left_var + join_op +
                    right_expression;
        }

    }

    /**
     * A branch node for a left outer join.  Using this node is a little non-
     * intuitive.  This node will only work when used in conjuction with
     * MarkerNode.
     * <p>
     * To use - first the complete left table in the join must be marked with a
     * name.  Then the ON expression is evaluated to a single plan node.  Then
     * this plan node must be added to result in a left outer join.  A tree for
     * a left outer join may look as follows;
     * <p><pre>
     *            LeftOuterJoinNode
     *                    |
     *                Join a = b
     *               /          \
     *          Marker       GetTable T2
     *            |
     *       GetTable T1
     * </pre>
     */
    public static class LeftOuterJoinNode extends SingleQueryPlanNode {

        static final long serialVersionUID = 8908801499550863492L;

        /**
         * The name of the mark that points to the left table that represents
         * the complete set.
         */
        private String complete_mark_name;

        public LeftOuterJoinNode(QueryPlanNode child, String complete_mark_name) {
            super(child);
            this.complete_mark_name = complete_mark_name;
        }

        public Table evaluate(QueryContext context) {
            // Evaluate the child branch,
            Table result = child.evaluate(context);
            // Get the table of the complete mark name,
            Table complete_left = context.getMarkedTable(complete_mark_name);

            // The rows in 'complete_left' that are outside (not in) the rows in the
            // left result.
            Table outside = complete_left.outside(result);

            // Create an OuterTable
            OuterTable outer_table = new OuterTable(result);
            outer_table.mergeIn(outside);

            // Return the outer table
            return outer_table;
        }

        public String titleString() {
            return "LEFT OUTER JOIN";
        }

    }

    /**
     * A branch node for a logical union of two tables of identical types.  This
     * branch can only work if the left and right children have exactly the same
     * ancestor tables.  If the ancestor tables are different it will fail.  This
     * node is used for logical OR.
     * <p>
     * This union does not include duplicated rows.
     */
    public static class LogicalUnionNode extends BranchQueryPlanNode {

        static final long serialVersionUID = -7783166856668779902L;

        public LogicalUnionNode(QueryPlanNode left, QueryPlanNode right) {
            super(left, right);
        }

        public Table evaluate(QueryContext context) {
            // Solve the left branch result
            Table left_result = left.evaluate(context);
            // Solve the right branch result
            Table right_result = right.evaluate(context);

            return left_result.union(right_result);
        }

        public String titleString() {
            return "LOGICAL UNION";
        }

    }

    /**
     * A branch node for performing a composite function on two child nodes.
     * This branch is used for general UNION, EXCEPT, INTERSECT composites.  The
     * left and right branch results must have the same number of columns and
     * column types.
     */
    public static class CompositeNode extends BranchQueryPlanNode {

        static final long serialVersionUID = -560587816928425857L;

        /**
         * The composite operation
         * (either CompositeTable.UNION, EXCEPT, INTERSECT).
         */
        private int composite_op;

        /**
         * If this is true, the composite includes all results from both children,
         * otherwise removes deplicates.
         */
        private boolean all_op;

        public CompositeNode(QueryPlanNode left, QueryPlanNode right,
                             int composite_op, boolean all_op) {
            super(left, right);
            this.composite_op = composite_op;
            this.all_op = all_op;
        }

        public Table evaluate(QueryContext context) {
            // Solve the left branch result
            Table left_result = left.evaluate(context);
            // Solve the right branch result
            Table right_result = right.evaluate(context);

            // Form the composite table
            CompositeTable t = new CompositeTable(left_result,
                    new Table[]{left_result, right_result});
            t.setupIndexesForCompositeFunction(composite_op, all_op);

            return t;
        }

    }

    /**
     * A branch node for a non-correlated ANY or ALL sub-query evaluation.  This
     * node requires a set of columns from the left branch and an operator.
     * The right branch represents the non-correlated sub-query.
     * <p>
     * NOTE: The cost of a SubQuery is higher if the right child result is
     *   greater than the left child result.  The plan should be arranged so
     *   smaller results are on the left.
     */
    public static class NonCorrelatedAnyAllNode extends BranchQueryPlanNode {

        static final long serialVersionUID = 7480579008259288291L;

        /**
         * The columns in the left table.
         */
        private Variable[] left_columns;

        /**
         * The SubQuery operator, eg. '= ANY', '<> ALL'
         */
        private Operator sub_query_operator;

        public NonCorrelatedAnyAllNode(QueryPlanNode left, QueryPlanNode right,
                                       Variable[] left_vars, Operator subquery_op) {
            super(left, right);
            this.left_columns = left_vars;
            this.sub_query_operator = subquery_op;
        }

        public Table evaluate(QueryContext context) {
            // Solve the left branch result
            Table left_result = left.evaluate(context);
            // Solve the right branch result
            Table right_result = right.evaluate(context);

            // Solve the sub query on the left columns with the right plan and the
            // given operator.
            return TableFunctions.anyAllNonCorrelated(left_result, left_columns,
                    sub_query_operator, right_result);
        }

        public Object clone() throws CloneNotSupportedException {
            NonCorrelatedAnyAllNode node = (NonCorrelatedAnyAllNode) super.clone();
            cloneArray(node.left_columns);
            return node;
        }

        public String titleString() {
            StringBuffer buf = new StringBuffer();
            buf.append("NON_CORRELATED: (");
            for (int i = 0; i < left_columns.length; ++i) {
                buf.append(left_columns[i].toString());
            }
            buf.append(") ");
            buf.append(sub_query_operator.toString());
            return new String(buf);
        }

    }

}
