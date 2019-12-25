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

import java.util.*;

import com.pony.database.*;

/**
 * The instance class that stores all the information about an update
 * statement for processing.
 *
 * @author Tobias Downer
 */

public class UpdateTable extends Statement {

    /**
     * The name the table that we are to update.
     */
    String table_name;

    /**
     * An array of Assignment objects which represent what we are changing.
     */
    ArrayList column_sets;

    /**
     * If the update statement has a 'where' clause, then this is set here.  If
     * it has no 'where' clause then we apply to the entire table.
     */
    SearchExpression where_condition;

    /**
     * The limit of the number of rows that are updated by this statement.  A
     * limit of -1 means there is no limit.
     */
    int limit = -1;

    /**
     * Tables that are relationally linked to the table being inserted into, set
     * after 'prepare'.  This is used to determine the tables we need to read
     * lock because we need to validate relational constraints on the tables.
     */
    private ArrayList relationally_linked_tables;


    // -----

    /**
     * The DataTable we are updating.
     */
    private DataTable update_table;

    /**
     * The TableName object set during 'prepare'.
     */
    private TableName tname;

    /**
     * The plan for the set of records we are updating in this query.
     */
    private QueryPlanNode plan;

    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {

        table_name = (String) cmd.getObject("table_name");
        column_sets = (ArrayList) cmd.getObject("assignments");
        where_condition = (SearchExpression) cmd.getObject("where_clause");
        limit = cmd.getInt("limit");

        // ---

        // Resolve the TableName object.
        tname = resolveTableName(table_name, database);
        // Does the table exist?
        if (!database.tableExists(tname)) {
            throw new DatabaseException("Table '" + tname + "' does not exist.");
        }
        // Get the table we are updating
        update_table = database.getTable(tname);

        // Form a TableSelectExpression that represents the select on the table
        TableSelectExpression select_expression = new TableSelectExpression();
        // Create the FROM clause
        select_expression.from_clause.addTable(table_name);
        // Set the WHERE clause
        select_expression.where_clause = where_condition;

        // Generate the TableExpressionFromSet hierarchy for the expression,
        TableExpressionFromSet from_set =
                Planner.generateFromSet(select_expression, database);
        // Form the plan
        plan = Planner.formQueryPlan(database, select_expression, from_set, null);

        // Resolve the variables in the assignments.
        for (int i = 0; i < column_sets.size(); ++i) {
            Assignment assignment = (Assignment) column_sets.get(i);
            Variable orig_var = assignment.getVariable();
            Variable new_var = from_set.resolveReference(orig_var);
            if (new_var == null) {
                throw new StatementException("Reference not found: " + orig_var);
            }
            orig_var.set(new_var);
            assignment.prepareExpressions(from_set.expressionQualifier());
        }

        // Resolve all tables linked to this
        TableName[] linked_tables =
                database.queryTablesRelationallyLinkedTo(tname);
        relationally_linked_tables = new ArrayList(linked_tables.length);
        for (int i = 0; i < linked_tables.length; ++i) {
            relationally_linked_tables.add(database.getTable(linked_tables[i]));
        }

    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        // Generate a list of Variable objects that represent the list of columns
        // being changed.
        Variable[] col_var_list = new Variable[column_sets.size()];
        for (int i = 0; i < col_var_list.length; ++i) {
            Assignment assign = (Assignment) column_sets.get(i);
            col_var_list[i] = assign.getVariable();
        }

        // Check that this user has privs to update the table.
        if (!database.getDatabase().canUserUpdateTableObject(context,
                user, tname, col_var_list)) {
            throw new UserAccessException(
                    "User not permitted to update table: " + table_name);
        }

        // Check the user has select permissions on the tables in the plan.
        Select.checkUserSelectPermissions(context, user, plan);

        // Evaluate the plan to find the update set.
        Table update_set = plan.evaluate(context);

        // Make an array of assignments
        Assignment[] assign_list = new Assignment[column_sets.size()];
        assign_list = (Assignment[]) column_sets.toArray(assign_list);
        // Update the data table.
        int update_count = update_table.update(context,
                update_set, assign_list, limit);

        // Notify TriggerManager that we've just done an update.
        if (update_count > 0) {
            database.notifyTriggerEvent(new TriggerEvent(
                    TriggerEvent.UPDATE, tname.toString(), update_count));
        }

        // Return the number of rows we updated.
        return FunctionTable.resultTable(context, update_count);

    }


}
