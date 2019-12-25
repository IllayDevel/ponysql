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

import java.util.Collections;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.pony.database.*;
import com.pony.util.BigNumber;

import java.util.Random;

/**
 * Various methods for forming query plans on SQL queries.
 *
 * @author Tobias Downer
 */

public class Planner {

    /**
     * The name of the GROUP BY function table.
     */
    private static final TableName GROUP_BY_FUNCTION_TABLE = new TableName(
            "FUNCTIONTABLE");

    /**
     * Used to generate unique marker names.
     */
    private static final Random marker_randomizer = new Random();


    /**
     * Returns a randomly generated outer join name.
     */
    private static String createRandomeOuterJoinName() {
        long v1 = marker_randomizer.nextLong();
        long v2 = marker_randomizer.nextLong();
        return "OUTER_JOIN_" +
                Long.toHexString(v1) + ":" + Long.toHexString(v2);
    }

    /**
     * Prepares the given SearchExpression object.  This goes through each
     * element of the Expression.  If the element is a variable it is qualified.
     * If the element is a TableSelectExpression it's converted to a
     * SelectQueriable object and prepared.
     */
    private static void prepareSearchExpression(
            final DatabaseConnection db, final TableExpressionFromSet from_set,
            SearchExpression expression) throws DatabaseException {
        // This is used to prepare sub-queries and qualify variables in a
        // search expression such as WHERE or HAVING.

        // Prepare the sub-queries first
        expression.prepare(new ExpressionPreparer() {
            public boolean canPrepare(Object element) {
                return element instanceof TableSelectExpression;
            }

            public Object prepare(Object element) throws DatabaseException {
                TableSelectExpression sq_expr = (TableSelectExpression) element;
                TableExpressionFromSet sq_from_set = generateFromSet(sq_expr, db);
                sq_from_set.setParent(from_set);
                QueryPlanNode sq_plan = formQueryPlan(db, sq_expr, sq_from_set, null);
                // Form this into a query plan type
                return new TObject(TType.QUERY_PLAN_TYPE,
                        new QueryPlan.CachePointNode(sq_plan));
            }
        });

        // Then qualify all the variables.  Note that this will not qualify
        // variables in the sub-queries.
        expression.prepare(from_set.expressionQualifier());

    }

    /**
     * Given a HAVING clause expression, this will generate a new HAVING clause
     * expression with all aggregate expressions put into the given extra
     * function list.
     */
    private static Expression filterHavingClause(Expression having_expr,
                                                 ArrayList aggregate_list,
                                                 QueryContext context) {
        if (having_expr.size() > 1) {
            Operator op = (Operator) having_expr.last();
            // If logical, split and filter the left and right expressions
            Expression[] exps = having_expr.split();
            Expression new_left =
                    filterHavingClause(exps[0], aggregate_list, context);
            Expression new_right =
                    filterHavingClause(exps[1], aggregate_list, context);
            Expression expr = new Expression(new_left, op, new_right);
            return expr;
        } else {
            // Not logical so determine if the expression is an aggregate or not
            if (having_expr.hasAggregateFunction(context)) {
                // Has aggregate functions so we must put this expression on the
                // aggregate list.
                aggregate_list.add(having_expr);
                // And substitute it with a variable reference.
                Variable v = Variable.resolve("FUNCTIONTABLE.HAVINGAG_" +
                        aggregate_list.size());
                return new Expression(v);
            } else {
                // No aggregate functions so leave it as is.
                return having_expr;
            }
        }

    }

    /**
     * Given a TableExpression, generates a TableExpressionFromSet object.  This
     * object is used to help qualify variable references.  This
     */
    static TableExpressionFromSet generateFromSet(
            TableSelectExpression select_expression, DatabaseConnection db) {

        // Get the 'from_clause' from the table expression
        FromClause from_clause = select_expression.from_clause;

        // Prepares the from_clause joining set.
        from_clause.getJoinSet().prepare(db);

        // Create a TableExpressionFromSet for this table expression
        TableExpressionFromSet from_set = new TableExpressionFromSet(db);

        // Add all tables from the 'from_clause'
        for (Object o : from_clause.allTables()) {
            FromTableDef ftdef = (FromTableDef) o;
            String unique_key = ftdef.getUniqueKey();
            String alias = ftdef.getAlias();

            // If this is a sub-query table,
            if (ftdef.isSubQueryTable()) {
                // eg. FROM ( SELECT id FROM Part )
                TableSelectExpression sub_query = ftdef.getTableSelectExpression();
                TableExpressionFromSet sub_query_from_set =
                        generateFromSet(sub_query, db);
                // The aliased name of the table
                TableName alias_table_name = null;
                if (alias != null) {
                    alias_table_name = new TableName(alias);
                }
                FromTableSubQuerySource source =
                        new FromTableSubQuerySource(db, unique_key, sub_query,
                                sub_query_from_set, alias_table_name);
                // Add to list of subquery tables to add to query,
                from_set.addTable(source);
            }
            // Else must be a standard query table,
            else {
                String name = ftdef.getName();

                // Resolve to full table name
                TableName table_name = db.resolveTableName(name);

                if (!db.tableExists(table_name)) {
                    throw new StatementException(
                            "Table '" + table_name + "' was not found.");
                }

                TableName given_name = null;
                if (alias != null) {
                    given_name = new TableName(alias);
                }

                // Get the TableQueryDef object for this table name (aliased).
                TableQueryDef table_query_def =
                        db.getTableQueryDef(table_name, given_name);
                FromTableDirectSource source = new FromTableDirectSource(db,
                        table_query_def, unique_key, given_name, table_name);

                from_set.addTable(source);
            }
        }  // while (tables.hasNext())

        // Set up functions, aliases and exposed variables for this from set,

        // The list of columns being selected (SelectColumn).
        ArrayList columns = select_expression.columns;

        // For each column being selected
        for (Object column : columns) {
            SelectColumn col = (SelectColumn) column;
            // Is this a glob?  (eg. Part.* )
            if (col.glob_name != null) {
                // Find the columns globbed and add to the 's_col_list' result.
                if (col.glob_name.equals("*")) {
                    from_set.exposeAllColumns();
                } else {
                    // Otherwise the glob must be of the form '[table name].*'
                    String tname =
                            col.glob_name.substring(0, col.glob_name.indexOf(".*"));
                    TableName tn = TableName.resolve(tname);
                    from_set.exposeAllColumnsFromSource(tn);
                }
            } else {
                // Otherwise must be a standard column reference.  Note that at this
                // time we aren't sure if a column expression is correlated and is
                // referencing an outer source.  This means we can't verify if the
                // column expression is valid or not at this point.

                // If this column is aliased, add it as a function reference to the
                // TableExpressionFromSet.
                String alias = col.alias;
                Variable v = col.expression.getVariable();
                boolean alias_match_v =
                        (v != null && alias != null &&
                                from_set.stringCompare(v.getName(), alias));
                if (alias != null && !alias_match_v) {
                    from_set.addFunctionRef(alias, col.expression);
                    from_set.exposeVariable(new Variable(alias));
                } else if (v != null) {
                    Variable resolved = from_set.resolveReference(v);
                    if (resolved == null) {
                        from_set.exposeVariable(v);
                    } else {
                        from_set.exposeVariable(resolved);
                    }
                } else {
                    String fun_name = col.expression.text().toString();
                    from_set.addFunctionRef(fun_name, col.expression);
                    from_set.exposeVariable(new Variable(fun_name));
                }
            }

        }  // for each column selected

        return from_set;
    }

    /**
     * Forms a query plan (QueryPlanNode) from the given TableSelectExpression
     * and TableExpressionFromSet.  The TableSelectExpression describes the
     * SELECT query (or sub-query), and the TableExpressionFromSet is used to
     * resolve expression references.
     * <p>
     * The 'order_by' argument is a list of ByColumn objects that represent
     * an optional ORDER BY clause.  If this is null or the list is empty, no
     * ordering is done.
     */
    public static QueryPlanNode formQueryPlan(DatabaseConnection db,
                                              TableSelectExpression expression, TableExpressionFromSet from_set,
                                              ArrayList order_by)
            throws DatabaseException {

        QueryContext context = new DatabaseQueryContext(db);

        // ----- Resolve the SELECT list

        // What we are selecting
        QuerySelectColumnSet column_set = new QuerySelectColumnSet(from_set);

        // The list of columns being selected (SelectColumn).
        ArrayList columns = expression.columns;

        // If there are 0 columns selected, then we assume the result should
        // show all of the columns in the result.
        boolean do_subset_column = (columns.size() != 0);

        // For each column being selected
        for (Object column : columns) {
            SelectColumn col = (SelectColumn) column;
            // Is this a glob?  (eg. Part.* )
            if (col.glob_name != null) {
                // Find the columns globbed and add to the 's_col_list' result.
                if (col.glob_name.equals("*")) {
                    column_set.selectAllColumnsFromAllSources();
                } else {
                    // Otherwise the glob must be of the form '[table name].*'
                    String tname =
                            col.glob_name.substring(0, col.glob_name.indexOf(".*"));
                    TableName tn = TableName.resolve(tname);
                    column_set.selectAllColumnsFromSource(tn);
                }
            } else {
                // Otherwise must be a standard column reference.
                column_set.selectSingleColumn(col);
            }

        }  // for each column selected

        // Prepare the column_set,
        column_set.prepare(context);

        // -----

        // Resolve any numerical references in the ORDER BY list (eg.
        // '1' will be a reference to column 1.

        if (order_by != null) {
            ArrayList prepared_col_set = column_set.s_col_list;
            for (Object o : order_by) {
                ByColumn col = (ByColumn) o;
                Expression exp = col.exp;
                if (exp.size() == 1) {
                    Object last_elem = exp.last();
                    if (last_elem instanceof TObject) {
                        BigNumber bnum = ((TObject) last_elem).toBigNumber();
                        if (bnum.getScale() == 0) {
                            int col_ref = bnum.intValue() - 1;
                            if (col_ref >= 0 && col_ref < prepared_col_set.size()) {
                                SelectColumn scol =
                                        (SelectColumn) prepared_col_set.get(col_ref);
                                col.exp = new Expression(scol.expression);
                            }
                        }
                    }
                }
            }
        }

        // -----

        // Set up plans for each table in the from clause of the query.  For
        // sub-queries, we recurse.

        QueryTableSetPlanner table_planner = new QueryTableSetPlanner();

        for (int i = 0; i < from_set.setCount(); ++i) {
            FromTableInterface table = from_set.getTable(i);
            if (table instanceof FromTableSubQuerySource) {
                // This represents a sub-query in the FROM clause

                FromTableSubQuerySource sq_table = (FromTableSubQuerySource) table;
                TableSelectExpression sq_expr = sq_table.getTableExpression();
                TableExpressionFromSet sq_from_set = sq_table.getFromSet();

                // Form a plan for evaluating the sub-query FROM
                QueryPlanNode sq_plan = formQueryPlan(db, sq_expr, sq_from_set, null);

                // The top should always be a SubsetNode,
                if (sq_plan instanceof QueryPlan.SubsetNode) {
                    QueryPlan.SubsetNode subset_node = (QueryPlan.SubsetNode) sq_plan;
                    subset_node.setGivenName(sq_table.getAliasedName());
                } else {
                    throw new RuntimeException("Top plan is not a SubsetNode!");
                }

                table_planner.addTableSource(sq_plan, sq_table);
            } else if (table instanceof FromTableDirectSource) {
                // This represents a direct referencable table in the FROM clause

                FromTableDirectSource ds_table = (FromTableDirectSource) table;
                TableName given_name = ds_table.getGivenTableName();
                TableName root_name = ds_table.getRootTableName();
                String aliased_name = null;
                if (!root_name.equals(given_name)) {
                    aliased_name = given_name.getName();
                }

                QueryPlanNode ds_plan = ds_table.createFetchQueryPlanNode();
                table_planner.addTableSource(ds_plan, ds_table);
            } else {
                throw new RuntimeException(
                        "Unknown table source instance: " + table.getClass());
            }

        }

        // -----

        // The WHERE and HAVING clauses
        SearchExpression where_clause = expression.where_clause;
        SearchExpression having_clause = expression.having_clause;

        // Look at the join set and resolve the ON Expression to this statement
        JoiningSet join_set = expression.from_clause.getJoinSet();

        // Perform a quick scan and see if there are any outer joins in the
        // expression.
        boolean all_inner_joins = true;
        for (int i = 0; i < join_set.getTableCount() - 1; ++i) {
            int type = join_set.getJoinType(i);
            if (type != JoiningSet.INNER_JOIN) {
                all_inner_joins = false;
            }
        }

        // Prepare the joins
        for (int i = 0; i < join_set.getTableCount() - 1; ++i) {
            int type = join_set.getJoinType(i);
            Expression on_expression = join_set.getOnExpression(i);

            if (all_inner_joins) {
                // If the whole join set is inner joins then simply move the on
                // expression (if there is one) to the WHERE clause.
                if (on_expression != null) {
                    where_clause.appendExpression(on_expression);
                }
            } else {
                // Not all inner joins,
                if (type == JoiningSet.INNER_JOIN && on_expression == null) {
                    // Regular join with no ON expression, so no preparation necessary
                } else {
                    // Either an inner join with an ON expression, or an outer join with
                    // ON expression
                    if (on_expression == null) {
                        throw new RuntimeException("No ON expression in join.");
                    }
                    // Resolve the on_expression
                    on_expression.prepare(from_set.expressionQualifier());
                    // And set it in the planner
                    table_planner.setJoinInfoBetweenSources(i, type, on_expression);
                }
            }

        }

        // Prepare the WHERE and HAVING clause, qualifies all variables and
        // prepares sub-queries.
        prepareSearchExpression(db, from_set, where_clause);
        prepareSearchExpression(db, from_set, having_clause);

        // Any extra AGGREGATE functions that are part of the HAVING clause that
        // we need to add.  This is a list of a name followed by the expression
        // that contains the aggregate function.
        ArrayList extra_aggregate_functions = new ArrayList();
        Expression new_having_clause = null;
        if (having_clause.getFromExpression() != null) {
            new_having_clause =
                    filterHavingClause(having_clause.getFromExpression(),
                            extra_aggregate_functions, context);
            having_clause.setFromExpression(new_having_clause);
        }

        // Any GROUP BY functions,
        ArrayList group_by_functions = new ArrayList();

        // Resolve the GROUP BY variable list references in this from set
        ArrayList group_list_in = expression.group_by;
        int gsz = group_list_in.size();
        Variable[] group_by_list = new Variable[gsz];
        for (int i = 0; i < gsz; ++i) {
            ByColumn by_column = (ByColumn) group_list_in.get(i);
            Expression exp = by_column.exp;
            // Prepare the group by expression
            exp.prepare(from_set.expressionQualifier());
            // Is the group by variable a complex expression?
            Variable v = exp.getVariable();

            Expression group_by_expression;
            if (v == null) {
                group_by_expression = exp;
            } else {
                // Can we dereference the variable to an expression in the SELECT?
                group_by_expression = from_set.dereferenceAssignment(v);
            }

            if (group_by_expression != null) {
                if (group_by_expression.hasAggregateFunction(context)) {
                    throw new StatementException("Aggregate expression '" +
                            group_by_expression.text().toString() +
                            "' is not allowed in GROUP BY clause.");
                }
                // Complex expression so add this to the function list.
                int group_by_fun_num = group_by_functions.size();
                group_by_functions.add(group_by_expression);
                v = new Variable(GROUP_BY_FUNCTION_TABLE,
                        "#GROUPBY-" + group_by_fun_num);
            }
            group_by_list[i] = v;
        }

        // Resolve GROUP MAX variable to a reference in this from set
        Variable groupmax_column = expression.group_max;
        if (groupmax_column != null) {
            Variable v = from_set.resolveReference(groupmax_column);
            if (v == null) {
                throw new StatementException("Could find GROUP MAX reference '" +
                        groupmax_column + "'");
            }
            groupmax_column = v;
        }

        // -----

        // Now all the variables should be resolved and correlated variables set
        // up as appropriate.

        // If nothing in the FROM clause then simply evaluate the result of the
        // select
        if (from_set.setCount() == 0) {
            if (column_set.aggregate_count > 0) {
                throw new StatementException(
                        "Invalid use of aggregate function in select with no FROM clause");
            }
            // Make up the lists
            ArrayList s_col_list = column_set.s_col_list;
            int sz = s_col_list.size();
            String[] col_names = new String[sz];
            Expression[] exp_list = new Expression[sz];
            Variable[] subset_vars = new Variable[sz];
            Variable[] aliases = new Variable[sz];
            for (int i = 0; i < sz; ++i) {
                SelectColumn scol = (SelectColumn) s_col_list.get(i);
                exp_list[i] = scol.expression;
                col_names[i] = scol.internal_name.getName();
                subset_vars[i] = new Variable(scol.internal_name);
                aliases[i] = new Variable(scol.resolved_name);
            }

            return new QueryPlan.SubsetNode(
                    new QueryPlan.CreateFunctionsNode(
                            new QueryPlan.SingleRowTableNode(), exp_list, col_names),
                    subset_vars, aliases);
        }

        // Plan the where clause.  The returned node is the plan to evaluate the
        // WHERE clause.
        QueryPlanNode node =
                table_planner.planSearchExpression(expression.where_clause);

        // Make up the functions list,
        ArrayList functions_list = column_set.function_col_list;
        int fsz = functions_list.size();
        ArrayList complete_fun_list = new ArrayList();
        for (Object o : functions_list) {
            SelectColumn scol = (SelectColumn) o;
            complete_fun_list.add(scol.expression);
            complete_fun_list.add(scol.internal_name.getName());
        }
        for (int i = 0; i < extra_aggregate_functions.size(); ++i) {
            complete_fun_list.add(extra_aggregate_functions.get(i));
            complete_fun_list.add("HAVINGAG_" + (i + 1));
        }

        int fsz2 = complete_fun_list.size() / 2;
        Expression[] def_fun_list = new Expression[fsz2];
        String[] def_fun_names = new String[fsz2];
        for (int i = 0; i < fsz2; ++i) {
            def_fun_list[i] = (Expression) complete_fun_list.get(i * 2);
            def_fun_names[i] = (String) complete_fun_list.get((i * 2) + 1);
        }

        // If there is more than 1 aggregate function or there is a group by
        // clause, then we must add a grouping plan.
        if (column_set.aggregate_count > 0 || gsz > 0) {

            // If there is no GROUP BY clause then assume the entire result is the
            // group.
            if (gsz == 0) {
                node = new QueryPlan.GroupNode(node, groupmax_column,
                        def_fun_list, def_fun_names);
            } else {
                // Do we have any group by functions that need to be planned first?
                int gfsz = group_by_functions.size();
                if (gfsz > 0) {
                    Expression[] group_fun_list = new Expression[gfsz];
                    String[] group_fun_name = new String[gfsz];
                    for (int i = 0; i < gfsz; ++i) {
                        group_fun_list[i] = (Expression) group_by_functions.get(i);
                        group_fun_name[i] = "#GROUPBY-" + i;
                    }
                    node = new QueryPlan.CreateFunctionsNode(node,
                            group_fun_list, group_fun_name);
                }

                // Otherwise we provide the 'group_by_list' argument
                node = new QueryPlan.GroupNode(node, group_by_list, groupmax_column,
                        def_fun_list, def_fun_names);

            }

        } else {
            // Otherwise no grouping is occuring.  We simply need create a function
            // node with any functions defined in the SELECT.
            // Plan a FunctionsNode with the functions defined in the SELECT.
            if (fsz > 0) {
                node = new QueryPlan.CreateFunctionsNode(node,
                        def_fun_list, def_fun_names);
            }
        }

        // The result column list
        ArrayList s_col_list = column_set.s_col_list;
        int sz = s_col_list.size();

        // Evaluate the having clause if necessary
        if (expression.having_clause.getFromExpression() != null) {
            // Before we evaluate the having expression we must substitute all the
            // aliased variables.
            Expression having_expr = having_clause.getFromExpression();
            substituteAliasedVariables(having_expr, s_col_list);

            PlanTableSource source = table_planner.getSingleTableSource();
            source.updatePlan(node);
            node = table_planner.planSearchExpression(having_clause);
        }

        // Do we have a composite select expression to process?
        QueryPlanNode right_composite = null;
        if (expression.next_composite != null) {
            TableSelectExpression composite_expr = expression.next_composite;
            // Generate the TableExpressionFromSet hierarchy for the expression,
            TableExpressionFromSet composite_from_set =
                    generateFromSet(composite_expr, db);

            // Form the right plan
            right_composite =
                    formQueryPlan(db, composite_expr, composite_from_set, null);

        }

        // Do we do a final subset column?
        Variable[] aliases = null;
        if (do_subset_column) {
            // Make up the lists
            Variable[] subset_vars = new Variable[sz];
            aliases = new Variable[sz];
            for (int i = 0; i < sz; ++i) {
                SelectColumn scol = (SelectColumn) s_col_list.get(i);
                subset_vars[i] = new Variable(scol.internal_name);
                aliases[i] = new Variable(scol.resolved_name);
            }

            // If we are distinct then add the DistinctNode here
            if (expression.distinct) {
                node = new QueryPlan.DistinctNode(node, subset_vars);
            }

            // Process the ORDER BY?
            // Note that the ORDER BY has to occur before the subset call, but
            // after the distinct because distinct can affect the ordering of the
            // result.
            if (right_composite == null && order_by != null) {
                node = planForOrderBy(node, order_by, from_set, s_col_list);
            }

            // Rename the columns as specified in the SELECT
            node = new QueryPlan.SubsetNode(node, subset_vars, aliases);

        } else {
            // Process the ORDER BY?
            if (right_composite == null && order_by != null) {
                node = planForOrderBy(node, order_by, from_set, s_col_list);
            }
        }

        // Do we have a composite to merge in?
        if (right_composite != null) {
            // For the composite
            node = new QueryPlan.CompositeNode(node, right_composite,
                    expression.composite_function, expression.is_composite_all);
            // Final order by?
            if (order_by != null) {
                node = planForOrderBy(node, order_by, from_set, s_col_list);
            }
            // Ensure a final subset node
            if (!(node instanceof QueryPlan.SubsetNode) && aliases != null) {
                node = new QueryPlan.SubsetNode(node, aliases, aliases);
            }

        }

        return node;
    }

    /**
     * Plans an ORDER BY set.  This is given its own function because we may
     * want to plan this at the end of a number of composite functions.
     * <p>
     * NOTE: s_col_list is optional.
     */
    public static QueryPlanNode planForOrderBy(QueryPlanNode plan,
                                               ArrayList order_by, TableExpressionFromSet from_set,
                                               ArrayList s_col_list)
            throws DatabaseException {

        TableName FUNCTION_TABLE = new TableName("FUNCTIONTABLE");

        // Sort on the ORDER BY clause
        if (order_by.size() > 0) {
            int sz = order_by.size();
            Variable[] order_list = new Variable[sz];
            boolean[] ascending_list = new boolean[sz];

            ArrayList function_orders = new ArrayList();

            for (int i = 0; i < sz; ++i) {
                ByColumn column = (ByColumn) order_by.get(i);
                Expression exp = column.exp;
                ascending_list[i] = column.ascending;
                Variable v = exp.getVariable();
                if (v != null) {
                    Variable new_v = from_set.resolveReference(v);
                    if (new_v == null) {
                        throw new StatementException(
                                "Can not resolve ORDER BY variable: " + v);
                    }
                    substituteAliasedVariable(new_v, s_col_list);

                    order_list[i] = new_v;
                } else {
                    // Otherwise we must be ordering by an expression such as
                    // '0 - a'.

                    // Resolve the expression,
                    exp.prepare(from_set.expressionQualifier());

                    // Make sure we substitute any aliased columns in the order by
                    // columns.
                    substituteAliasedVariables(exp, s_col_list);

                    // The new ordering functions are called 'FUNCTIONTABLE.#ORDER-n'
                    // where n is the number of the ordering expression.
                    order_list[i] =
                            new Variable(FUNCTION_TABLE, "#ORDER-" + function_orders.size());
                    function_orders.add(exp);
                }

//        System.out.println(exp);

            }

            // If there are functional orderings,
            // For this we must define a new FunctionTable with the expressions,
            // then order by those columns, and then use another SubsetNode
            // query node.
            int fsz = function_orders.size();
            if (fsz > 0) {
                Expression[] funs = new Expression[fsz];
                String[] fnames = new String[fsz];
                for (int n = 0; n < fsz; ++n) {
                    funs[n] = (Expression) function_orders.get(n);
                    fnames[n] = "#ORDER-" + n;
                }

                if (plan instanceof QueryPlan.SubsetNode) {
                    // If the top plan is a QueryPlan.SubsetNode then we use the
                    //   information from it to create a new SubsetNode that
                    //   doesn't include the functional orders we have attached here.
                    QueryPlan.SubsetNode top_subset_node = (QueryPlan.SubsetNode) plan;
                    Variable[] mapped_names = top_subset_node.getNewColumnNames();

                    // Defines the sort functions
                    plan = new QueryPlan.CreateFunctionsNode(plan, funs, fnames);
                    // Then plan the sort
                    plan = new QueryPlan.SortNode(plan, order_list, ascending_list);
                    // Then plan the subset
                    plan = new QueryPlan.SubsetNode(plan, mapped_names, mapped_names);
                } else {
                    // Defines the sort functions
                    plan = new QueryPlan.CreateFunctionsNode(plan, funs, fnames);
                    // Plan the sort
                    plan = new QueryPlan.SortNode(plan, order_list, ascending_list);
                }

            } else {
                // No functional orders so we only need to sort by the columns
                // defined.
                plan = new QueryPlan.SortNode(plan, order_list, ascending_list);
            }

        }

        return plan;
    }

    /**
     * Substitutes any aliased variables in the given expression with the
     * function name equivalent.  For example, if we have a 'SELECT 3 + 4 Bah'
     * then resolving on variable Bah will be subsituted to the function column
     * that represents the result of 3 + 4.
     */
    private static void substituteAliasedVariables(
            Expression expression, ArrayList s_col_list) {
        List all_vars = expression.allVariables();
        for (Object all_var : all_vars) {
            Variable v = (Variable) all_var;
            substituteAliasedVariable(v, s_col_list);
        }
    }

    private static void substituteAliasedVariable(Variable v,
                                                  ArrayList s_col_list) {
        if (s_col_list != null) {
            int sz = s_col_list.size();
            for (Object o : s_col_list) {
                SelectColumn scol = (SelectColumn) o;
                if (v.equals(scol.resolved_name)) {
                    v.set(scol.internal_name);
                }
            }
        }
    }


    // ---------- Inner classes ----------

    /**
     * A container object for the set of SelectColumn objects selected in a
     * query.
     */
    private static class QuerySelectColumnSet {

        /**
         * The name of the table where functions are defined.
         */
        private static final TableName FUNCTION_TABLE_NAME =
                new TableName("FUNCTIONTABLE");

        /**
         * The tables we are selecting from.
         */
        private final TableExpressionFromSet from_set;

        /**
         * The list of SelectColumn.
         */
        final ArrayList s_col_list;

        /**
         * The list of functions in this column set.
         */
        final ArrayList function_col_list;

        /**
         * The current number of 'FUNCTIONTABLE.' columns in the table.  This is
         * incremented for each custom column.
         */
        private int running_fun_number = 0;

        /**
         * The count of aggregate and constant columns included in the result set.
         * Aggregate columns are, (count(*), avg(cost_of) * 0.75, etc).  Constant
         * columns are, (9 * 4, 2, (9 * 7 / 4) + 4, etc).
         */
        int aggregate_count = 0, constant_count = 0;

        /**
         * Constructor.
         */
        public QuerySelectColumnSet(TableExpressionFromSet from_set) {
            this.from_set = from_set;
            s_col_list = new ArrayList();
            function_col_list = new ArrayList();
        }

        /**
         * Adds a single SelectColumn to the list of output columns from the
         * query.
         * <p>
         * Note that at this point the the information in the given SelectColumn
         * may not be completely qualified.
         */
        void selectSingleColumn(SelectColumn col) {
            s_col_list.add(col);
        }

        /**
         * Adds all columns from the given FromTableInterface object.
         */
        void addAllFromTable(FromTableInterface table) {
            // Select all the tables
            Variable[] vars = table.allColumns();
            int s_col_list_max = s_col_list.size();
            for (Variable v : vars) {
                // The Variable
                // Make up the SelectColumn
                SelectColumn ncol = new SelectColumn();
                Expression e = new Expression(v);
                e.text().append(v.toString());
                ncol.alias = null;
                ncol.expression = e;
                ncol.resolved_name = v;
                ncol.internal_name = v;

                // Add to the list of columns selected
                selectSingleColumn(ncol);
            }
        }

        /**
         * Adds all column from the given table object.  This is used to set up the
         * columns that are to be viewed as the result of the select statement.
         */
        void selectAllColumnsFromSource(TableName table_name) {
            // Attempt to find the table in the from set.
            FromTableInterface table = from_set.findTable(
                    table_name.getSchema(), table_name.getName());
            if (table == null) {
                throw new StatementException(table_name.toString() +
                        ".* is not a valid reference.");
            }

            addAllFromTable(table);
        }

        /**
         * Sets up this queriable with all columns from all table sources.
         */
        void selectAllColumnsFromAllSources() {
            for (int p = 0; p < from_set.setCount(); ++p) {
                FromTableInterface table = from_set.getTable(p);
                addAllFromTable(table);
            }
        }

        /**
         * Adds a new hidden function into the column set.  This is intended
         * to be used for implied functions.  For example, a query may have a
         * function in a GROUP BY clause.  It's desirable to include the function
         * in the column set but not in the final result.
         * <p>
         * Returns an absolute Variable object that can be used to reference
         * this hidden column.
         */
        Variable addHiddenFunction(String fun_alias, Expression function,
                                   QueryContext context) {
            SelectColumn scol = new SelectColumn();
            scol.resolved_name = new Variable(fun_alias);
            scol.alias = fun_alias;
            scol.expression = function;
            scol.internal_name = new Variable(FUNCTION_TABLE_NAME, fun_alias);

            // If this is an aggregate column then add to aggregate count.
            if (function.hasAggregateFunction(context)) {
                ++aggregate_count;
            }
            // If this is a constant column then add to constant cound.
            else if (function.isConstant()) {
                ++constant_count;
            }

            function_col_list.add(scol);

            return scol.internal_name;
        }

        /**
         * Prepares the given SelectColumn by fully qualifying the expression and
         * allocating it correctly within this context.
         */
        private void prepareSelectColumn(SelectColumn col, QueryContext context)
                throws DatabaseException {
            // Check to see if we have any Select statements in the
            //   Expression.  This is necessary, because we can't have a
            //   sub-select evaluated during list table downloading.
            List exp_elements = col.expression.allElements();
            for (Object exp_element : exp_elements) {
                if (exp_element instanceof TableSelectExpression) {
                    throw new StatementException(
                            "Sub-query not allowed in column list.");
                }
            }

            // First fully qualify the select expression
            col.expression.prepare(from_set.expressionQualifier());

            // If the expression isn't a simple variable, then add to
            // function list.
            Variable v = col.expression.getVariable();
            if (v == null) {
                // This means we have a complex expression.

                ++running_fun_number;
                String agg_str = Integer.toString(running_fun_number);

                // If this is an aggregate column then add to aggregate count.
                if (col.expression.hasAggregateFunction(context)) {
                    ++aggregate_count;
                    // Add '_A' code to end of internal name of column to signify this is
                    // an aggregate column
                    agg_str += "_A";
                }
                // If this is a constant column then add to constant cound.
                else if (col.expression.isConstant()) {
                    ++constant_count;
                } else {
                    // Must be an expression with variable's embedded ( eg.
                    //   (part_id + 3) * 4, (id * value_of_part), etc )
                }
                function_col_list.add(col);

                col.internal_name = new Variable(FUNCTION_TABLE_NAME, agg_str);
                if (col.alias == null) {
                    col.alias = new String(col.expression.text());
                }
                col.resolved_name = new Variable(col.alias);

            } else {
                // Not a complex expression
                col.internal_name = v;
                if (col.alias == null) {
                    col.resolved_name = v;
                } else {
                    col.resolved_name = new Variable(col.alias);
                }
            }

        }


        /**
         * Resolves all variable objects in each column.
         */
        void prepare(QueryContext context) throws DatabaseException {
            // Prepare each of the columns selected.
            // NOTE: A side-effect of this is that it qualifies all the Expressions
            //   that are functions in TableExpressionFromSet.  After this section,
            //   we can dereference variables for their function Expression.
            for (Object o : s_col_list) {
                SelectColumn column = (SelectColumn) o;
                prepareSelectColumn(column, context);
            }
        }


    }


    /**
     * A table set planner that maintains a list of table dependence lists and
     * progressively constructs a plan tree from the bottom up.
     */
    private static class QueryTableSetPlanner {

        /**
         * The list of PlanTableSource objects for each source being planned.
         */
        private ArrayList table_list;

        /**
         * If a join has occurred since the planner was constructed or copied then
         * this is set to true.
         */
        private boolean has_join_occurred;


        /**
         * Constructor.
         */
        public QueryTableSetPlanner() {
            this.table_list = new ArrayList();
            has_join_occurred = false;
        }

        /**
         * Add a PlanTableSource to this planner.
         */
        private void addPlanTableSource(PlanTableSource source) {
            table_list.add(source);
            has_join_occurred = true;
        }

        /**
         * Returns true if a join has occurred ('table_list' has been modified).
         */
        public boolean hasJoinOccured() {
            return has_join_occurred;
        }

        /**
         * Adds a new table source to the planner given a Plan that 'creates'
         * the source, and a FromTableInterface that describes the source created
         * by the plan.
         */
        public void addTableSource(QueryPlanNode plan,
                                   FromTableInterface from_def) {
            Variable[] all_cols = from_def.allColumns();
            String[] unique_names = new String[]{from_def.getUniqueName()};
            addPlanTableSource(new PlanTableSource(plan, all_cols, unique_names));
        }

        /**
         * Returns the index of the given PlanTableSource in the table list.
         */
        private int indexOfPlanTableSource(PlanTableSource source) {
            int sz = table_list.size();
            for (int i = 0; i < sz; ++i) {
                if (table_list.get(i) == source) {
                    return i;
                }
            }
            return -1;
        }

        /**
         * Links the last added table source to the previous added table source
         * through this joining information.
         * <p>
         * 'between_index' represents the point in between the table sources that
         * the join should be setup for.  For example, to set the join between
         * TableSource 0 and 1, use 0 as the between index.  A between index of 3
         * represents the join between TableSource index 2 and 2.
         */
        public void setJoinInfoBetweenSources(
                int between_index, int join_type, Expression on_expr) {
            PlanTableSource plan_left =
                    (PlanTableSource) table_list.get(between_index);
            PlanTableSource plan_right =
                    (PlanTableSource) table_list.get(between_index + 1);
            plan_left.setRightJoinInfo(plan_right, join_type, on_expr);
            plan_right.setLeftJoinInfo(plan_left, join_type, on_expr);
        }

        /**
         * Forms a new PlanTableSource that's the concatination of the given
         * two PlanTableSource objects.
         */
        public static PlanTableSource concatTableSources(
                PlanTableSource left, PlanTableSource right, QueryPlanNode plan) {

            // Merge the variable list
            Variable[] new_var_list = new Variable[left.var_list.length +
                    right.var_list.length];
            int i = 0;
            for (int n = 0; n < left.var_list.length; ++n) {
                new_var_list[i] = left.var_list[n];
                ++i;
            }
            for (int n = 0; n < right.var_list.length; ++n) {
                new_var_list[i] = right.var_list[n];
                ++i;
            }

            // Merge the unique table names list
            String[] new_unique_list = new String[left.unique_names.length +
                    right.unique_names.length];
            i = 0;
            for (int n = 0; n < left.unique_names.length; ++n) {
                new_unique_list[i] = left.unique_names[n];
                ++i;
            }
            for (int n = 0; n < right.unique_names.length; ++n) {
                new_unique_list[i] = right.unique_names[n];
                ++i;
            }

            // Return the new table source plan.
            return new PlanTableSource(plan, new_var_list, new_unique_list);
        }

        /**
         * Joins two tables when a plan is generated for joining the two tables.
         */
        public PlanTableSource mergeTables(
                PlanTableSource left, PlanTableSource right,
                QueryPlanNode merge_plan) {

            // Remove the sources from the table list.
            table_list.remove(left);
            table_list.remove(right);
            // Add the concatination of the left and right tables.
            PlanTableSource c_plan = concatTableSources(left, right, merge_plan);
            c_plan.setJoinInfoMergedBetween(left, right);
            c_plan.setUpdated();
            addPlanTableSource(c_plan);
            // Return the name plan
            return c_plan;
        }

        /**
         * Finds and returns the PlanTableSource in the list of tables that
         * contains the given Variable reference.
         */
        public PlanTableSource findTableSource(Variable ref) {
            int sz = table_list.size();

            // If there is only 1 plan then assume the variable is in there.
            if (sz == 1) {
                return (PlanTableSource) table_list.get(0);
            }

            for (Object o : table_list) {
                PlanTableSource source = (PlanTableSource) o;
                if (source.containsVariable(ref)) {
                    return source;
                }
            }
            throw new RuntimeException(
                    "Unable to find table with variable reference: " + ref);
        }

        /**
         * Finds a common PlanTableSource that contains the list of variables
         * given.  If the list is 0 or there is no common source then null is
         * returned.
         */
        public PlanTableSource findCommonTableSource(List var_list) {
            if (var_list.size() == 0) {
                return null;
            }

            PlanTableSource plan = findTableSource((Variable) var_list.get(0));
            int i = 1;
            int sz = var_list.size();
            while (i < sz) {
                PlanTableSource p2 = findTableSource((Variable) var_list.get(i));
                if (plan != p2) {
                    return null;
                }
                ++i;
            }

            return plan;
        }

        /**
         * Finds and returns the PlanTableSource in the list of table that
         * contains the given unique key name.
         */
        public PlanTableSource findTableSourceWithUniqueKey(String key) {
            int sz = table_list.size();
            for (Object o : table_list) {
                PlanTableSource source = (PlanTableSource) o;
                if (source.containsUniqueKey(key)) {
                    return source;
                }
            }
            throw new RuntimeException(
                    "Unable to find table with unique key: " + key);
        }

        /**
         * Returns the single PlanTableSource for this planner.
         */
        private PlanTableSource getSingleTableSource() {
            if (table_list.size() != 1) {
                throw new RuntimeException("Not a single table source.");
            }
            return (PlanTableSource) table_list.get(0);
        }

        /**
         * Sets a CachePointNode with the given key on all of the plan table
         * sources in 'table_list'.  Note that this does not change the 'update'
         * status of the table sources.  If there is currently a CachePointNode
         * on any of the sources then no update is made.
         */
        private void setCachePoints() {
            int sz = table_list.size();
            for (Object o : table_list) {
                PlanTableSource plan = (PlanTableSource) o;
                if (!(plan.getPlan() instanceof QueryPlan.CachePointNode)) {
                    plan.plan = new QueryPlan.CachePointNode(plan.getPlan());
                }
            }
        }

        /**
         * Creates a single PlanTableSource that encapsulates all the given
         * variables in a single table.  If this means a table must be joined with
         * another using the natural join conditions then this happens here.
         * <p>
         * The intention of this function is to produce a plan that encapsulates
         * all the variables needed to perform a specific evaluation.
         * <p>
         * Note, this has the potential to cause 'natural join' situations which
         * are bad performance.  It is a good idea to perform joins using other
         * methods before this is used.
         * <p>
         * Note, this will change the 'table_list' variable in this class if tables
         * are joined.
         */
        private PlanTableSource joinAllPlansWithVariables(List all_vars) {
            // Collect all the plans that encapsulate these variables.
            ArrayList touched_plans = new ArrayList();
            int sz = all_vars.size();
            for (Object all_var : all_vars) {
                Variable v = (Variable) all_var;
                PlanTableSource plan = findTableSource(v);
                if (!touched_plans.contains(plan)) {
                    touched_plans.add(plan);
                }
            }
            // Now 'touched_plans' contains a list of PlanTableSource for each
            // plan to be joined.

            return joinAllPlansToSingleSource(touched_plans);
        }

        /**
         * Returns true if it is possible to naturally join the two plans.  Two
         * plans can be joined under the following sitations;
         * 1) The left or right plan of the first source points to the second
         *    source.
         * 2) Either one has no left plan and the other has no right plan, or
         *    one has no right plan and the other has no left plan.
         */
        private int canPlansBeNaturallyJoined(
                PlanTableSource plan1, PlanTableSource plan2) {
            if (plan1.left_plan == plan2 || plan1.right_plan == plan2) {
                return 0;
            } else if (plan1.left_plan != null && plan2.left_plan != null) {
                // This is a left clash
                return 2;
            } else if (plan1.right_plan != null && plan2.right_plan != null) {
                // This is a right clash
                return 1;
            } else if ((plan1.left_plan == null && plan2.right_plan == null) ||
                    (plan1.right_plan == null && plan2.left_plan == null)) {
                // This means a merge between the plans is fine
                return 0;
            } else {
                // Must be a left and right clash
                return 2;
            }
        }

        /**
         * Given a list of PlanTableSource objects, this will produce a plan that
         * naturally joins all the tables together into a single plan.  The
         * join algorithm used is determined by the information in the FROM clause.
         * An OUTER JOIN, for example, will join depending on the conditions
         * provided in the ON clause.  If no explicit join method is provided then
         * a natural join will be planned.
         * <p>
         * Care should be taken with this because this method can produce natural
         * joins which are often optimized out by more appropriate join expressions
         * that can be processed before this is called.
         * <p>
         * Note, this will change the 'table_list' variable in this class if tables
         * are joined.
         * <p>
         * Returns null if no plans are provided.
         */
        private PlanTableSource joinAllPlansToSingleSource(List all_plans) {
            // If there are no plans then return null
            if (all_plans.size() == 0) {
                return null;
            }
            // Return early if there is only 1 table.
            else if (all_plans.size() == 1) {
                return (PlanTableSource) all_plans.get(0);
            }

            // Make a working copy of the plan list.
            ArrayList working_plan_list = new ArrayList(all_plans.size());
            for (Object all_plan : all_plans) {
                working_plan_list.add(all_plan);
            }

            // We go through each plan in turn.
            while (working_plan_list.size() > 1) {
                PlanTableSource left_plan = (PlanTableSource) working_plan_list.get(0);
                PlanTableSource right_plan =
                        (PlanTableSource) working_plan_list.get(1);
                // First we need to determine if the left and right plan can be
                // naturally joined.
                int status = canPlansBeNaturallyJoined(left_plan, right_plan);
                if (status == 0) {
                    // Yes they can so join them
                    PlanTableSource new_plan = naturallyJoinPlans(left_plan, right_plan);
                    // Remove the left and right plan from the list and add the new plan
                    working_plan_list.remove(left_plan);
                    working_plan_list.remove(right_plan);
                    working_plan_list.add(0, new_plan);
                } else if (status == 1) {
                    // No we can't because of a right join clash, so we join the left
                    // plan right in hopes of resolving the clash.
                    PlanTableSource new_plan =
                            naturallyJoinPlans(left_plan, left_plan.right_plan);
                    working_plan_list.remove(left_plan);
                    working_plan_list.remove(left_plan.right_plan);
                    working_plan_list.add(0, new_plan);
                } else if (status == 2) {
                    // No we can't because of a left join clash, so we join the left
                    // plan left in hopes of resolving the clash.
                    PlanTableSource new_plan =
                            naturallyJoinPlans(left_plan, left_plan.left_plan);
                    working_plan_list.remove(left_plan);
                    working_plan_list.remove(left_plan.left_plan);
                    working_plan_list.add(0, new_plan);
                } else {
                    throw new RuntimeException("Unknown status: " + status);
                }
            }

            // Return the working plan of the merged tables.
            return (PlanTableSource) working_plan_list.get(0);

        }

        /**
         * Naturally joins two PlanTableSource objects in this planner.  When this
         * method returns the actual plans will be joined together.  This method
         * modifies 'table_list'.
         */
        private PlanTableSource naturallyJoinPlans(
                PlanTableSource plan1, PlanTableSource plan2) {
            int join_type;
            Expression on_expr;
            PlanTableSource left_plan, right_plan;
            // Are the plans linked by common join information?
            if (plan1.right_plan == plan2) {
                join_type = plan1.right_join_type;
                on_expr = plan1.right_on_expr;
                left_plan = plan1;
                right_plan = plan2;
            } else if (plan1.left_plan == plan2) {
                join_type = plan1.left_join_type;
                on_expr = plan1.left_on_expr;
                left_plan = plan2;
                right_plan = plan1;
            } else {
                // Assertion - make sure no join clashes!
                if ((plan1.left_plan != null && plan2.left_plan != null) ||
                        (plan1.right_plan != null && plan2.right_plan != null)) {
                    throw new RuntimeException(
                            "Assertion failed - plans can not be naturally join because " +
                                    "the left/right join plans clash.");
                }

                // Else we must assume a non-dependant join (not an outer join).
                // Perform a natural join
                QueryPlanNode node = new QueryPlan.NaturalJoinNode(
                        plan1.getPlan(), plan2.getPlan());
                return mergeTables(plan1, plan2, node);
            }

            // This means plan1 and plan2 are linked by a common join and ON
            // expression which we evaluate now.
            String outer_join_name;
            if (join_type == JoiningSet.LEFT_OUTER_JOIN) {
                outer_join_name = createRandomeOuterJoinName();
                // Mark the left plan
                left_plan.updatePlan(new QueryPlan.MarkerNode(
                        left_plan.getPlan(), outer_join_name));
            } else if (join_type == JoiningSet.RIGHT_OUTER_JOIN) {
                outer_join_name = createRandomeOuterJoinName();
                // Mark the right plan
                right_plan.updatePlan(new QueryPlan.MarkerNode(
                        right_plan.getPlan(), outer_join_name));
            } else if (join_type == JoiningSet.INNER_JOIN) {
                // Inner join with ON expression
                outer_join_name = null;
            } else {
                throw new RuntimeException(
                        "Join type (" + join_type + ") is not supported.");
            }

            // Make a Planner object for joining these plans.
            QueryTableSetPlanner planner = new QueryTableSetPlanner();
            planner.addPlanTableSource(left_plan.copy());
            planner.addPlanTableSource(right_plan.copy());

//      planner.printDebugInfo();

            // Evaluate the on expression
            QueryPlanNode node = planner.logicalEvaluate(on_expr);
            // If outer join add the left outer join node
            if (outer_join_name != null) {
                node = new QueryPlan.LeftOuterJoinNode(node, outer_join_name);
            }
            // And merge the plans in this set with the new node.
            return mergeTables(plan1, plan2, node);

//      System.out.println("OUTER JOIN: " + on_expr);
//      throw new RuntimeException("PENDING");

        }

        /**
         * Plans all outer joins.
         * <p>
         * Note, this will change the 'table_list' variable in this class if tables
         * are joined.
         */
        private void planAllOuterJoins() {
//      new Error().printStackTrace();

            int sz = table_list.size();
            if (sz <= 1) {
                return;
            }

            // Make a working copy of the plan list.
            ArrayList working_plan_list = new ArrayList(sz);
            for (Object o : table_list) {
                working_plan_list.add(o);
            }

//      System.out.println("----");

            PlanTableSource plan1 = (PlanTableSource) working_plan_list.get(0);
            for (int i = 1; i < sz; ++i) {
                PlanTableSource plan2 = (PlanTableSource) working_plan_list.get(i);

//        System.out.println("Joining: " + plan1);
//        System.out.println("   with: " + plan2);

                if (plan1.right_plan == plan2) {
                    plan1 = naturallyJoinPlans(plan1, plan2);
                } else {
                    plan1 = plan2;
                }
            }

        }

        /**
         * Naturally joins all remaining tables sources to make a final single
         * plan which is returned.
         * <p>
         * Note, this will change the 'table_list' variable in this class if tables
         * are joined.
         */
        private PlanTableSource naturalJoinAll() {
            int sz = table_list.size();
            if (sz == 1) {
                return (PlanTableSource) table_list.get(0);
            }

            // Produce a plan that naturally joins all tables.
            return joinAllPlansToSingleSource(table_list);
        }

        /**
         * Convenience class that stores an expression to evaluate for a table.
         */
        private static class SingleVarPlan {
            PlanTableSource table_source;
            Variable single_var;
            Variable variable;
            Expression expression;
        }

        /**
         * Adds a single var plan to the given list.
         */
        private void addSingleVarPlanTo(ArrayList list, PlanTableSource table,
                                        Variable variable, Variable single_var,
                                        Expression[] exp_parts, Operator op) {
            Expression exp = new Expression(exp_parts[0], op, exp_parts[1]);
            // Is this source in the list already?
            int sz = list.size();
            for (Object o : list) {
                SingleVarPlan plan = (SingleVarPlan) o;
                if (plan.table_source == table &&
                        (variable == null || plan.variable.equals(variable))) {
                    // Append to end of current expression
                    plan.variable = variable;
                    plan.expression = new Expression(plan.expression,
                            Operator.get("and"), exp);
                    return;
                }
            }
            // Didn't find so make a new entry in the list.
            SingleVarPlan plan = new SingleVarPlan();
            plan.table_source = table;
            plan.variable = variable;
            plan.single_var = single_var;
            plan.expression = exp;
            list.add(plan);
            return;
        }

        // ----

        // An expression plan for a constant expression.  These are very
        // optimizable indeed.
        private class ConstantExpressionPlan extends ExpressionPlan {
            private final Expression expression;

            public ConstantExpressionPlan(Expression e) {
                expression = e;
            }

            public void addToPlanTree() {
                // Each currently open branch must have this constant expression added
                // to it.
                for (Object o : table_list) {
                    PlanTableSource plan = (PlanTableSource) o;
                    plan.updatePlan(new QueryPlan.ConstantSelectNode(
                            plan.getPlan(), expression));
                }
            }
        }

        private class SimpleSelectExpressionPlan extends ExpressionPlan {
            private final Variable single_var;
            private final Operator op;
            private final Expression expression;

            public SimpleSelectExpressionPlan(Variable v, Operator op,
                                              Expression e) {
                single_var = v;
                this.op = op;
                expression = e;
            }

            public void addToPlanTree() {
                // Find the table source for this variable
                PlanTableSource table_source = findTableSource(single_var);
                table_source.updatePlan(new QueryPlan.SimpleSelectNode(
                        table_source.getPlan(), single_var, op, expression));
            }
        }

        private class SimpleSingleExpressionPlan extends ExpressionPlan {
            private final Variable single_var;
            private final Expression expression;

            public SimpleSingleExpressionPlan(Variable v, Expression e) {
                single_var = v;
                expression = e;
            }

            public void addToPlanTree() {
                // Find the table source for this variable
                PlanTableSource table_source = findTableSource(single_var);
                table_source.updatePlan(new QueryPlan.RangeSelectNode(
                        table_source.getPlan(), expression));
            }
        }

        private class ComplexSingleExpressionPlan extends ExpressionPlan {
            private final Variable single_var;
            private final Expression expression;

            public ComplexSingleExpressionPlan(Variable v, Expression e) {
                single_var = v;
                expression = e;
            }

            public void addToPlanTree() {
                // Find the table source for this variable
                PlanTableSource table_source = findTableSource(single_var);
                table_source.updatePlan(new QueryPlan.ExhaustiveSelectNode(
                        table_source.getPlan(), expression));
            }
        }

        private class SimplePatternExpressionPlan extends ExpressionPlan {
            private final Variable single_var;
            private final Expression expression;

            public SimplePatternExpressionPlan(Variable v, Expression e) {
                single_var = v;
                expression = e;
            }

            public void addToPlanTree() {
                // Find the table source for this variable
                PlanTableSource table_source = findTableSource(single_var);
                table_source.updatePlan(new QueryPlan.SimplePatternSelectNode(
                        table_source.getPlan(), expression));
            }
        }

        private class ExhaustiveSelectExpressionPlan extends ExpressionPlan {
            private final Expression expression;

            public ExhaustiveSelectExpressionPlan(Expression e) {
                expression = e;
            }

            public void addToPlanTree() {
                // Get all the variables of this expression.
                List all_vars = expression.allVariables();
                // Find the table source for this set of variables.
                PlanTableSource table_source = joinAllPlansWithVariables(all_vars);
                // Perform the exhaustive select
                table_source.updatePlan(new QueryPlan.ExhaustiveSelectNode(
                        table_source.getPlan(), expression));
            }
        }

        private class ExhaustiveSubQueryExpressionPlan extends ExpressionPlan {
            private final List all_vars;
            private final Expression expression;

            public ExhaustiveSubQueryExpressionPlan(List vars, Expression e) {
                this.all_vars = vars;
                this.expression = e;
            }

            public void addToPlanTree() {
                PlanTableSource table_source = joinAllPlansWithVariables(all_vars);
                // Update the plan
                table_source.updatePlan(new QueryPlan.ExhaustiveSelectNode(
                        table_source.getPlan(), expression));

            }
        }

        private class SimpleSubQueryExpressionPlan extends ExpressionPlan {
            private final Expression expression;

            public SimpleSubQueryExpressionPlan(Expression e) {
                this.expression = e;
            }

            public void addToPlanTree() {
                Operator op = (Operator) expression.last();
                Expression[] exps = expression.split();
                Variable left_var = exps[0].getVariable();
                QueryPlanNode right_plan = exps[1].getQueryPlanNode();

                // Find the table source for this variable
                PlanTableSource table_source = findTableSource(left_var);
                // The left branch
                QueryPlanNode left_plan = table_source.getPlan();
                // Update the plan
                table_source.updatePlan(
                        new QueryPlan.NonCorrelatedAnyAllNode(
                                left_plan, right_plan, new Variable[]{left_var}, op));
            }
        }

        private class ExhaustiveJoinExpressionPlan extends ExpressionPlan {
            private final Expression expression;

            public ExhaustiveJoinExpressionPlan(Expression e) {
                this.expression = e;
            }

            public void addToPlanTree() {
                // Get all the variables in the expression
                List all_vars = expression.allVariables();
                // Merge it into one plan (possibly performing natural joins).
                PlanTableSource all_plan = joinAllPlansWithVariables(all_vars);
                // And perform the exhaustive select,
                all_plan.updatePlan(new QueryPlan.ExhaustiveSelectNode(
                        all_plan.getPlan(), expression));
            }
        }

        private class StandardJoinExpressionPlan extends ExpressionPlan {
            private final Expression expression;

            public StandardJoinExpressionPlan(Expression e) {
                this.expression = e;
            }

            public void addToPlanTree() {

                // Get the expression with the multiple variables
                Expression[] exps = expression.split();

                // Get the list of variables in the left hand and right hand side
                Variable lhs_v = exps[0].getVariable();
                Variable rhs_v = exps[1].getVariable();
                List lhs_vars = exps[0].allVariables();
                List rhs_vars = exps[1].allVariables();

                // Get the operator
                Operator op = (Operator) expression.last();

                // Get the left and right plan for the variables in the expression.
                // Note that these methods may perform natural joins on the table.
                PlanTableSource lhs_plan = joinAllPlansWithVariables(lhs_vars);
                PlanTableSource rhs_plan = joinAllPlansWithVariables(rhs_vars);

                // If the lhs and rhs plans are different (there is a joining
                // situation).
                if (lhs_plan != rhs_plan) {

                    // If either the LHS or the RHS is a single variable then we can
                    // optimize the join.

                    if (lhs_v != null || rhs_v != null) {
                        // If rhs_v is a single variable and lhs_v is not then we must
                        // reverse the expression.
                        QueryPlan.JoinNode join_node;
                        if (lhs_v == null && rhs_v != null) {
                            // Reverse the expressions and the operator
                            join_node = new QueryPlan.JoinNode(
                                    rhs_plan.getPlan(), lhs_plan.getPlan(),
                                    rhs_v, op.reverse(), exps[0]);
                            mergeTables(rhs_plan, lhs_plan, join_node);
                        } else {
                            // Otherwise, use it as it is.
                            join_node = new QueryPlan.JoinNode(
                                    lhs_plan.getPlan(), rhs_plan.getPlan(),
                                    lhs_v, op, exps[1]);
                            mergeTables(lhs_plan, rhs_plan, join_node);
                        }
                        // Return because we are done
                        return;
                    }

                } // if lhs and rhs plans are different

                // If we get here either both the lhs and rhs are complex expressions
                // or the lhs and rhs of the variable are not different plans, or
                // the operator is not a conditional.  Either way, we must evaluate
                // this via a natural join of the variables involved coupled with an
                // exhaustive select.  These types of queries are poor performing.

                // Get all the variables in the expression
                List all_vars = expression.allVariables();
                // Merge it into one plan (possibly performing natural joins).
                PlanTableSource all_plan = joinAllPlansWithVariables(all_vars);
                // And perform the exhaustive select,
                all_plan.updatePlan(new QueryPlan.ExhaustiveSelectNode(
                        all_plan.getPlan(), expression));
            }
        }

        private class SubLogicExpressionPlan extends ExpressionPlan {
            private final Expression expression;

            public SubLogicExpressionPlan(Expression e) {
                this.expression = e;
            }

            public void addToPlanTree() {
                planForExpression(expression);
            }
        }


        /**
         * Evaluates a list of constant conditional exressions of the form
         * '3 + 2 = 0', 'true = true', etc.
         */
        void evaluateConstants(ArrayList constant_vars, ArrayList evaluate_order) {
            // For each constant variable
            for (Object constant_var : constant_vars) {
                Expression expr = (Expression) constant_var;
                // Add the exression plan
                ExpressionPlan exp_plan = new ConstantExpressionPlan(expr);
                exp_plan.setOptimizableValue(0f);
                evaluate_order.add(exp_plan);
            }
        }

        /**
         * Evaluates a list of single variable conditional expressions of the
         * form a = 3, a > 1 + 2, a - 2 = 1, 3 = a, concat(a, 'a') = '3a', etc.
         * The rule is there must be only one variable, a conditional operator,
         * and a constant on one side.
         * <p>
         * This method takes the list and modifies the plan as necessary.
         */
        void evaluateSingles(ArrayList single_vars, ArrayList evaluate_order) {

            // The list of simple expression plans (lhs = single)
            ArrayList simple_plan_list = new ArrayList();
            // The list of complex function expression plans (lhs = expression)
            ArrayList complex_plan_list = new ArrayList();

            // For each single variable expression
            for (Object singleVar : single_vars) {
                Expression andexp = (Expression) singleVar;
                // The operator
                Operator op = (Operator) andexp.last();

                // Split the expression
                Expression[] exps = andexp.split();
                // The single var
                Variable single_var;

                // If the operator is a sub-query we must be of the form,
                // 'a in ( 1, 2, 3 )'
                if (op.isSubQuery()) {
                    single_var = exps[0].getVariable();
                    if (single_var != null) {
                        ExpressionPlan exp_plan = new SimpleSelectExpressionPlan(
                                single_var, op, exps[1]);
                        exp_plan.setOptimizableValue(0.2f);
                        evaluate_order.add(exp_plan);
                    } else {
                        single_var = (Variable) exps[0].allVariables().get(0);
                        ExpressionPlan exp_plan = new ComplexSingleExpressionPlan(
                                single_var, andexp);
                        exp_plan.setOptimizableValue(0.8f);
                        evaluate_order.add(exp_plan);
                    }
                } else {
                    // Put the variable on the LHS, constant on the RHS
                    List all_vars = exps[0].allVariables();
                    if (all_vars.size() == 0) {
                        // Reverse the expressions and the operator
                        Expression temp_exp = exps[0];
                        exps[0] = exps[1];
                        exps[1] = temp_exp;
                        op = op.reverse();
                        single_var = (Variable) exps[0].allVariables().get(0);
                    } else {
                        single_var = (Variable) all_vars.get(0);
                    }
                    // The table source
                    PlanTableSource table_source = findTableSource(single_var);
                    // Simple LHS?
                    Variable v = exps[0].getVariable();
                    if (v != null) {
                        addSingleVarPlanTo(simple_plan_list, table_source, v,
                                single_var, exps, op);
                    } else {
                        // No, complex lhs
                        addSingleVarPlanTo(complex_plan_list, table_source, null,
                                single_var, exps, op);
                    }
                }
            }

            // We now have a list of simple and complex plans for each table,
            int sz = simple_plan_list.size();
            for (int i = 0; i < sz; ++i) {
                SingleVarPlan var_plan = (SingleVarPlan) simple_plan_list.get(i);
                ExpressionPlan exp_plan = new SimpleSingleExpressionPlan(
                        var_plan.single_var, var_plan.expression);
                exp_plan.setOptimizableValue(0.2f);
                evaluate_order.add(exp_plan);
            }

            sz = complex_plan_list.size();
            for (int i = 0; i < sz; ++i) {
                SingleVarPlan var_plan = (SingleVarPlan) complex_plan_list.get(i);
                ExpressionPlan exp_plan = new ComplexSingleExpressionPlan(
                        var_plan.single_var, var_plan.expression);
                exp_plan.setOptimizableValue(0.8f);
                evaluate_order.add(exp_plan);
            }

        }

        /**
         * Evaluates a list of expressions that are pattern searches (eg. LIKE,
         * NOT LIKE and REGEXP).  Note that the LHS or RHS may be complex
         * expressions with variables, but we are guarenteed that there are
         * no sub-expressions in the expression.
         */
        void evaluatePatterns(ArrayList pattern_exprs, ArrayList evaluate_order) {

            // Split the patterns into simple and complex plans.  A complex plan
            // may require that a join occurs.

            for (Object pattern_expr : pattern_exprs) {
                Expression expr = (Expression) pattern_expr;

                Expression[] exps = expr.split();
                // If the LHS is a single variable and the RHS is a constant then
                // the conditions are right for a simple pattern search.
                Variable lhs_v = exps[0].getVariable();
                if (expr.isConstant()) {
                    ExpressionPlan expr_plan = new ConstantExpressionPlan(expr);
                    expr_plan.setOptimizableValue(0f);
                    evaluate_order.add(expr_plan);
                } else if (lhs_v != null && exps[1].isConstant()) {
                    ExpressionPlan expr_plan =
                            new SimplePatternExpressionPlan(lhs_v, expr);
                    expr_plan.setOptimizableValue(0.25f);
                    evaluate_order.add(expr_plan);
                } else {
                    // Otherwise we must assume a complex pattern search which may
                    // require a join.  For example, 'a + b LIKE 'a%'' or
                    // 'a LIKE b'.  At the very least, this will be an exhaustive
                    // search and at the worst it will be a join + exhaustive search.
                    // So we should evaluate these at the end of the evaluation order.
                    ExpressionPlan expr_plan = new ExhaustiveSelectExpressionPlan(expr);
                    expr_plan.setOptimizableValue(0.82f);
                    evaluate_order.add(expr_plan);
                }

            }

        }

        /**
         * Evaluates a list of expressions containing sub-queries.  Non-correlated
         * sub-queries can often be optimized in to fast searches.  Correlated
         * queries, or expressions containing multiple sub-queries are put
         * through the ExhaustiveSelect plan.
         */
        void evaluateSubQueries(ArrayList expressions, ArrayList evaluate_order) {

            // For each sub-query expression
            for (Object expression : expressions) {
                Expression andexp = (Expression) expression;

                boolean is_exhaustive;
                Variable left_var = null;
                QueryPlanNode right_plan = null;

                // Is this an easy sub-query?
                Operator op = (Operator) andexp.last();
                if (op.isSubQuery()) {
                    // Split the expression.
                    Expression[] exps = andexp.split();
                    // Check that the left is a simple enough variable reference
                    left_var = exps[0].getVariable();
                    if (left_var != null) {
                        // Check that the right is a sub-query plan.
                        right_plan = exps[1].getQueryPlanNode();
                        if (right_plan != null) {
                            // Finally, check if the plan is correlated or not
                            ArrayList cv =
                                    right_plan.discoverCorrelatedVariables(1, new ArrayList());
//              System.out.println("Right Plan: " + right_plan);
//              System.out.println("Correlated variables: " + cv);
                            // No correlated variables so we are a standard, non-correlated
                            // query!
                            is_exhaustive = cv.size() != 0;
                        } else {
                            is_exhaustive = true;
                        }
                    } else {
                        is_exhaustive = true;
                    }
                } else {
                    // Must be an exhaustive sub-query
                    is_exhaustive = true;
                }

                // If this is an exhaustive operation,
                if (is_exhaustive) {
                    // This expression could involve multiple variables, so we may need
                    // to join.
                    List all_vars = andexp.allVariables();

                    // Also find all correlated variables.
                    List all_correlated =
                            andexp.discoverCorrelatedVariables(0, new ArrayList());
                    int sz = all_correlated.size();

                    // If there are no variables (and no correlated variables) then this
                    // must be a constant select, For example, 3 in ( select ... )
                    if (all_vars.size() == 0 && sz == 0) {
                        ExpressionPlan expr_plan = new ConstantExpressionPlan(andexp);
                        expr_plan.setOptimizableValue(0f);
                        evaluate_order.add(expr_plan);
                    } else {

                        for (Object o : all_correlated) {
                            CorrelatedVariable cv =
                                    (CorrelatedVariable) o;
                            all_vars.add(cv.getVariable());
                        }

                        // An exhaustive expression plan which might require a join or a
                        // slow correlated search.  This should be evaluated after the
                        // multiple variables are processed.
                        ExpressionPlan exp_plan = new ExhaustiveSubQueryExpressionPlan(
                                all_vars, andexp);
                        exp_plan.setOptimizableValue(0.85f);
                        evaluate_order.add(exp_plan);
                    }

                } else {

                    // This is a simple sub-query expression plan with a single LHS
                    // variable and a single RHS sub-query.
                    ExpressionPlan exp_plan = new SimpleSubQueryExpressionPlan(andexp);
                    exp_plan.setOptimizableValue(0.3f);
                    evaluate_order.add(exp_plan);

                }

            } // For each 'and' expression

        }

        /**
         * Evaluates a list of expressions containing multiple variable expression.
         * For example, 'a = b', 'a > b + c', 'a + 5 * b = 2', etc.  If an
         * expression represents a simple join condition then a join plan is
         * made to the query plan tree.  If an expression represents a more
         * complex joining condition then an exhaustive search must be used.
         */
        void evaluateMultiples(ArrayList multi_vars, ArrayList evaluate_order) {

            // FUTURE OPTIMIZATION:
            //   This join order planner is a little primitive in design.  It orders
            //   optimizable joins first and least optimizable last, but does not
            //   take into account other factors that we could use to optimize
            //   joins in the future.

            // For each single variable expression
            for (Object multi_var : multi_vars) {

                // Get the expression with the multiple variables
                Expression expr = (Expression) multi_var;
                Expression[] exps = expr.split();

                // Get the list of variables in the left hand and right hand side
                Variable lhs_v = exps[0].getVariable();
                Variable rhs_v = exps[1].getVariable();

                // Work out how optimizable the join is.
                // The calculation is as follows;
                // a) If both the lhs and rhs are a single variable then the
                //    optimizable value is set to 0.6f.
                // b) If only one of lhs or rhs is a single variable then the
                //    optimizable value is set to 0.64f.
                // c) Otherwise it is set to 0.68f (exhaustive select guarenteed).

                if (lhs_v == null && rhs_v == null) {
                    // Neither lhs or rhs are single vars
                    ExpressionPlan exp_plan = new ExhaustiveJoinExpressionPlan(expr);
                    exp_plan.setOptimizableValue(0.68f);
                    evaluate_order.add(exp_plan);
                } else if (lhs_v != null && rhs_v != null) {
                    // Both lhs and rhs are a single var (most optimizable type of
                    // join).
                    ExpressionPlan exp_plan = new StandardJoinExpressionPlan(expr);
                    exp_plan.setOptimizableValue(0.60f);
                    evaluate_order.add(exp_plan);
                } else {
                    // Either lhs or rhs is a single var
                    ExpressionPlan exp_plan = new StandardJoinExpressionPlan(expr);
                    exp_plan.setOptimizableValue(0.64f);
                    evaluate_order.add(exp_plan);
                }

            } // for each expression we are 'and'ing against

        }

        /**
         * Evaluates a list of expressions that are sub-expressions themselves.
         * This is typically called when we have OR queries in the expression.
         */
        void evaluateSubLogic(ArrayList sublogic_exprs, ArrayList evaluate_order) {

            each_logic_expr:
            for (Object sublogic_expr : sublogic_exprs) {
                Expression expr = (Expression) sublogic_expr;

                // Break the expression down to a list of OR expressions,
                ArrayList or_exprs = expr.breakByOperator(new ArrayList(), "or");

                // An optimizations here;

                // If all the expressions we are ORing together are in the same table
                // then we should execute them before the joins, otherwise they
                // should go after the joins.

                // The reason for this is because if we can lesson the amount of work a
                // join has to do then we should.  The actual time it takes to perform
                // an OR search shouldn't change if it is before or after the joins.

                PlanTableSource common = null;

                for (Object orExpr : or_exprs) {
                    Expression or_expr = (Expression) orExpr;
                    List vars = or_expr.allVariables();
                    // If there are no variables then don't bother with this expression
                    if (vars.size() > 0) {
                        // Find the common table source (if any)
                        PlanTableSource ts = findCommonTableSource(vars);
                        boolean or_after_joins = false;
                        if (ts == null) {
                            // No common table, so OR after the joins
                            or_after_joins = true;
                        } else if (common == null) {
                            common = ts;
                        } else if (common != ts) {
                            // No common table with the vars in this OR list so do this OR
                            // after the joins.
                            or_after_joins = true;
                        }

                        if (or_after_joins) {
                            ExpressionPlan exp_plan = new SubLogicExpressionPlan(expr);
                            exp_plan.setOptimizableValue(0.70f);
                            evaluate_order.add(exp_plan);
                            // Continue to the next logic expression
                            continue each_logic_expr;
                        }
                    }
                }

                // Either we found a common table or there are no variables in the OR.
                // Either way we should evaluate this after the join.
                ExpressionPlan exp_plan = new SubLogicExpressionPlan(expr);
                exp_plan.setOptimizableValue(0.58f);
                evaluate_order.add(exp_plan);

            }

        }


        // -----

        /**
         * Generates a plan to evaluate the given list of expressions
         * (logically separated with AND).
         */
        void planForExpressionList(List and_list) {

            ArrayList sub_logic_expressions = new ArrayList();
            // The list of expressions that have a sub-select in them.
            ArrayList sub_query_expressions = new ArrayList();
            // The list of all constant expressions ( true = true )
            ArrayList constants = new ArrayList();
            // The list of pattern matching expressions (eg. 't LIKE 'a%')
            ArrayList pattern_expressions = new ArrayList();
            // The list of all expressions that are a single variable on one
            // side, a conditional operator, and a constant on the other side.
            ArrayList single_vars = new ArrayList();
            // The list of multi variable expressions (possible joins)
            ArrayList multi_vars = new ArrayList();

            // Separate out each condition type.
            for (Object el : and_list) {
                Expression andexp = (Expression) el;
                // If we end with a logical operator then we must recurse them
                // through this method.
                Object lob = andexp.last();
                Operator op;
                // If the last is not an operator, then we imply
                // '[expression] = true'
                if (!(lob instanceof Operator) ||
                        ((Operator) lob).isMathematical()) {
                    Operator EQUAL_OP = Operator.get("=");
                    andexp.addElement(TObject.booleanVal(true));
                    andexp.addOperator(EQUAL_OP);
                    op = EQUAL_OP;
                } else {
                    op = (Operator) lob;
                }
                // If the last is logical (eg. AND, OR) then we must process the
                // sub logic expression
                if (op.isLogical()) {
                    sub_logic_expressions.add(andexp);
                }
                // Does the expression have a sub-query?  (eg. Another select
                //   statement somewhere in it)
                else if (andexp.hasSubQuery()) {
                    sub_query_expressions.add(andexp);
                } else if (op.isPattern()) {
                    pattern_expressions.add(andexp);
                } else { //if (op.isCondition()) {
                    // The list of variables in the expression.
                    List vars = andexp.allVariables();
                    if (vars.size() == 0) {
                        // These are ( 54 + 9 = 9 ), ( "z" > "a" ), ( 9.01 - 2 ), etc
                        constants.add(andexp);
                    } else if (vars.size() == 1) {
                        // These are ( id = 90 ), ( 'a' < number ), etc
                        single_vars.add(andexp);
                    } else if (vars.size() > 1) {
                        // These are ( id = part_id ),
                        // ( cost_of + value_of < sold_at ), ( id = part_id - 10 )
                        multi_vars.add(andexp);
                    } else {
                        throw new Error("Hmm, vars list size is negative!");
                    }
                }
            }

            // The order in which expression are evaluated,
            // (ExpressionPlan)
            ArrayList evaluate_order = new ArrayList();

            // Evaluate the constants.  These should always be evaluated first
            // because they always evaluate to either true or false or null.
            evaluateConstants(constants, evaluate_order);

            // Evaluate the singles.  If formed well these can be evaluated
            // using fast indices.  eg. (a > 9 - 3) is more optimal than
            // (a + 3 > 9).
            evaluateSingles(single_vars, evaluate_order);

            // Evaluate the pattern operators.  Note that some patterns can be
            // optimized better than others, but currently we keep this near the
            // middle of our evaluation sequence.
            evaluatePatterns(pattern_expressions, evaluate_order);

            // Evaluate the sub-queries.  These are queries of the form,
            // (a IN ( SELECT ... )), (a = ( SELECT ... ) = ( SELECT ... )), etc.
            evaluateSubQueries(sub_query_expressions, evaluate_order);

            // Evaluate multiple variable expressions.  It's possible these are
            // joins.
            evaluateMultiples(multi_vars, evaluate_order);

            // Lastly evaluate the sub-logic expressions.  These expressions are
            // OR type expressions.
            evaluateSubLogic(sub_logic_expressions, evaluate_order);


            // Sort the evaluation list by how optimizable the expressions are,
            Collections.sort(evaluate_order);
            // And add each expression to the plan
            for (Object o : evaluate_order) {
                ExpressionPlan plan = (ExpressionPlan) o;
                plan.addToPlanTree();
            }

        }

        /**
         * Evaluates the search Expression clause and alters the banches of
         * the plans in this object as necessary.  Unlike the 'logicalEvaluate'
         * method, this does not result in a single QueryPlanNode.  It is the
         * responsibility of the callee to join branches as required.
         */
        void planForExpression(Expression exp) {
            if (exp == null) {
                return;
            }

            Object ob = exp.last();
            if (ob instanceof Operator && ((Operator) ob).isLogical()) {
                Operator last_op = (Operator) ob;

                if (last_op.is("or")) {
                    // parsing an OR block
                    // Split left and right of logical operator.
                    Expression[] exps = exp.split();
                    // If we are an 'or' then evaluate left and right and union the
                    // result.

                    // Before we branch set cache points.
                    setCachePoints();

                    // Make copies of the left and right planner
                    QueryTableSetPlanner left_planner = copy();
                    QueryTableSetPlanner right_planner = copy();

                    // Plan the left and right side of the OR
                    left_planner.planForExpression(exps[0]);
                    right_planner.planForExpression(exps[1]);

                    // Fix the left and right planner so that they represent the same
                    // 'group'.
                    // The current implementation naturally joins all sources if the
                    // number of sources is different than the original size.
                    int left_sz = left_planner.table_list.size();
                    int right_sz = right_planner.table_list.size();
                    if (left_sz != right_sz ||
                            left_planner.hasJoinOccured() ||
                            right_planner.hasJoinOccured()) {
                        // Naturally join all in the left and right plan
                        left_planner.naturalJoinAll();
                        right_planner.naturalJoinAll();
                    }

                    // Union all table sources, but only if they have changed.
                    ArrayList left_table_list = left_planner.table_list;
                    ArrayList right_table_list = right_planner.table_list;
                    int sz = left_table_list.size();

                    // First we must determine the plans that need to be joined in the
                    // left and right plan.
                    ArrayList left_join_list = new ArrayList();
                    ArrayList right_join_list = new ArrayList();
                    for (int i = 0; i < sz; ++i) {
                        PlanTableSource left_plan =
                                (PlanTableSource) left_table_list.get(i);
                        PlanTableSource right_plan =
                                (PlanTableSource) right_table_list.get(i);
                        if (left_plan.isUpdated() || right_plan.isUpdated()) {
                            left_join_list.add(left_plan);
                            right_join_list.add(right_plan);
                        }
                    }

                    // Make sure the plans are joined in the left and right planners
                    left_planner.joinAllPlansToSingleSource(left_join_list);
                    right_planner.joinAllPlansToSingleSource(right_join_list);

                    // Since the planner lists may have changed we update them here.
                    left_table_list = left_planner.table_list;
                    right_table_list = right_planner.table_list;
                    sz = left_table_list.size();

                    ArrayList new_table_list = new ArrayList(sz);

                    for (int i = 0; i < sz; ++i) {
                        PlanTableSource left_plan =
                                (PlanTableSource) left_table_list.get(i);
                        PlanTableSource right_plan =
                                (PlanTableSource) right_table_list.get(i);

                        PlanTableSource new_plan;

                        // If left and right plan updated so we need to union them
                        if (left_plan.isUpdated() || right_plan.isUpdated()) {

                            // In many causes, the left and right branches will contain
                            //   identical branches that would best be optimized out.

                            // Take the left plan, add the logical union to it, and make it
                            // the plan for this.
                            QueryPlanNode node = new QueryPlan.LogicalUnionNode(
                                    left_plan.getPlan(), right_plan.getPlan());

                            // Update the plan in this table list
                            left_plan.updatePlan(node);

                            new_plan = left_plan;
                        } else {
                            // If the left and right plan didn't update, then use the
                            // left plan (it doesn't matter if we use left or right because
                            // they are the same).
                            new_plan = left_plan;
                        }

                        // Add the left plan to the new table list we are creating
                        new_table_list.add(new_plan);

                    }

                    // Set the new table list
                    table_list = new_table_list;

                } else if (last_op.is("and")) {
                    // parsing an AND block
                    // The list of AND expressions that are here
                    ArrayList and_list = new ArrayList();
                    and_list = createAndList(and_list, exp);

                    planForExpressionList(and_list);

                } else {
                    throw new RuntimeException("Unknown logical operator: " + ob);
                }

            } else {
                // Not a logical expression so just plan for this single expression.
                ArrayList exp_list = new ArrayList(1);
                exp_list.add(exp);
                planForExpressionList(exp_list);
            }

        }

        /**
         * Evaluates a search Expression clause.  Note that is some cases this
         * will generate a plan tree that has many identical branches that can be
         * optimized out.
         */
        QueryPlanNode logicalEvaluate(Expression exp) {

//      System.out.println("Logical Evaluate: " + exp);

            if (exp == null) {
                // Naturally join everything and return the plan.
                naturalJoinAll();
                // Return the plan
                return getSingleTableSource().getPlan();
            }

            // Plan the expression
            planForExpression(exp);

            // Naturally join any straggling tables
            naturalJoinAll();

            // Return the plan
            return getSingleTableSource().getPlan();
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
         * Evalutes the WHERE clause of the table expression.
         */
        QueryPlanNode planSearchExpression(SearchExpression search_expression) {
            // First perform all outer tables.
            planAllOuterJoins();

            QueryPlanNode node =
                    logicalEvaluate(search_expression.getFromExpression());
            return node;
        }

        /**
         * Makes an exact duplicate copy (deep clone) of this planner object.
         */
        private QueryTableSetPlanner copy() {
            QueryTableSetPlanner copy = new QueryTableSetPlanner();
            int sz = table_list.size();
            for (int i = 0; i < sz; ++i) {
                copy.table_list.add(((PlanTableSource) table_list.get(i)).copy());
            }
            // Copy the left and right links in the PlanTableSource
            for (int i = 0; i < sz; ++i) {
                PlanTableSource src = (PlanTableSource) table_list.get(i);
                PlanTableSource mod = (PlanTableSource) copy.table_list.get(i);
                // See how the left plan links to which index,
                if (src.left_plan != null) {
                    int n = indexOfPlanTableSource(src.left_plan);
                    mod.setLeftJoinInfo((PlanTableSource) copy.table_list.get(n),
                            src.left_join_type, src.left_on_expr);
                }
                // See how the right plan links to which index,
                if (src.right_plan != null) {
                    int n = indexOfPlanTableSource(src.right_plan);
                    mod.setRightJoinInfo((PlanTableSource) copy.table_list.get(n),
                            src.right_join_type, src.right_on_expr);
                }
            }

            return copy;
        }

        void printDebugInfo() {
            StringBuffer buf = new StringBuffer();
            buf.append("PLANNER:\n");
            for (int i = 0; i < table_list.size(); ++i) {
                buf.append("TABLE " + i + "\n");
                ((PlanTableSource) table_list.get(i)).getPlan().debugString(2, buf);
                buf.append("\n");
            }
            System.out.println(buf);
        }


    }

    /**
     * Represents a single table source being planned.
     */
    private static class PlanTableSource {

        /**
         * The Plan for this table source.
         */
        private QueryPlanNode plan;

        /**
         * The list of fully qualified Variable objects that are accessable within
         * this plan.
         */
        private final Variable[] var_list;

        /**
         * The list of unique key names of the tables in this plan.
         */
        private final String[] unique_names;

        /**
         * Set to true when this source has been updated from when it was
         * constructed or copied.
         */
        private boolean is_updated;

        /**
         * How this plan is naturally joined to other plans in the source.  A
         * plan either has no dependance, a left or a right dependance, or a left
         * and right dependance.
         */
        PlanTableSource left_plan, right_plan;
        int left_join_type, right_join_type;
        Expression left_on_expr, right_on_expr;


        /**
         * Constructor.
         */
        public PlanTableSource(QueryPlanNode plan, Variable[] var_list,
                               String[] table_unique_names) {
            this.plan = plan;
            this.var_list = var_list;
            this.unique_names = table_unique_names;
            left_join_type = -1;
            right_join_type = -1;
            is_updated = false;
        }

        /**
         * Sets the left join information for this plan.
         */
        void setLeftJoinInfo(PlanTableSource left_plan,
                             int join_type, Expression on_expr) {
            this.left_plan = left_plan;
            this.left_join_type = join_type;
            this.left_on_expr = on_expr;
        }

        /**
         * Sets the right join information for this plan.
         */
        void setRightJoinInfo(PlanTableSource right_plan,
                              int join_type, Expression on_expr) {
            this.right_plan = right_plan;
            this.right_join_type = join_type;
            this.right_on_expr = on_expr;
        }

        /**
         * This is called when two plans are merged together to set up the
         * left and right join information for the new plan.  This sets the left
         * join info from the left plan and the right join info from the right
         * plan.
         */
        void setJoinInfoMergedBetween(
                PlanTableSource left, PlanTableSource right) {

            if (left.right_plan != right) {
                if (left.right_plan != null) {
                    setRightJoinInfo(left.right_plan,
                            left.right_join_type, left.right_on_expr);
                    right_plan.left_plan = this;
                }
                if (right.left_plan != null) {
                    setLeftJoinInfo(right.left_plan,
                            right.left_join_type, right.left_on_expr);
                    left_plan.right_plan = this;
                }
            }
            if (left.left_plan != right) {
                if (left_plan == null && left.left_plan != null) {
                    setLeftJoinInfo(left.left_plan,
                            left.left_join_type, left.left_on_expr);
                    left_plan.right_plan = this;
                }
                if (right_plan == null && right.right_plan != null) {
                    setRightJoinInfo(right.right_plan,
                            right.right_join_type, right.right_on_expr);
                    right_plan.left_plan = this;
                }
            }

        }

        /**
         * Returns true if this table source contains the variable reference.
         */
        public boolean containsVariable(Variable v) {
//      System.out.println("Looking for: " + v);
            for (Variable variable : var_list) {
//        System.out.println(var_list[i]);
                if (variable.equals(v)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Returns true if this table source contains the unique table name
         * reference.
         */
        public boolean containsUniqueKey(String name) {
            for (String unique_name : unique_names) {
                if (unique_name.equals(name)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Sets the updated flag.
         */
        public void setUpdated() {
            is_updated = true;
        }

        /**
         * Updates the plan.
         */
        public void updatePlan(QueryPlanNode node) {
            plan = node;
            setUpdated();
        }

        /**
         * Returns the plan for this table source.
         */
        public QueryPlanNode getPlan() {
            return plan;
        }

        /**
         * Returns true if the planner was updated.
         */
        public boolean isUpdated() {
            return is_updated;
        }

        /**
         * Makes a copy of this table source.
         */
        public PlanTableSource copy() {
            return new PlanTableSource(plan, var_list, unique_names);
        }

    }

    /**
     * An abstract class that represents an expression to be added into a plan.
     * Many sets of expressions can be added into the plan tree in any order,
     * however it is often desirable to add some more intensive expressions
     * higher up the branches.  This object allows us to order expressions by
     * optimization value.  More optimizable expressions are put near the leafs
     * of the plan tree and least optimizable and put near the top.
     */
    static abstract class ExpressionPlan implements Comparable {

        /**
         * How optimizable an expression is.  A value of 0 indicates most
         * optimizable and 1 indicates least optimizable.
         */
        private float optimizable_value;

        /**
         * Sets the optimizable value of this plan.
         */
        public void setOptimizableValue(float v) {
            optimizable_value = v;
        }

        /**
         * Returns the optimizable value for this plan.
         */
        public float getOptimizableValue() {
            return optimizable_value;
        }

        /**
         * Adds this expression into the plan tree.
         */
        public abstract void addToPlanTree();

        public int compareTo(Object ob) {
            ExpressionPlan dest_plan = (ExpressionPlan) ob;
            float dest_val = dest_plan.optimizable_value;
            return Float.compare(optimizable_value, dest_val);
        }

    }

}
