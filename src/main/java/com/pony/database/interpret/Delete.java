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
 * Logic for the DELETE FROM SQL statement.
 *
 * @author Tobias Downer
 */

public class Delete extends Statement {

    /**
     * The name the table that we are to delete from.
     */
    String table_name;

    /**
     * If the delete statement has a 'where' clause, then this is set here.  If
     * it has no 'where' clause then we apply to the entire table.
     */
    SearchExpression where_condition;

    /**
     * The limit of the number of rows that are updated by this statement.  A
     * limit of < 0 means there is no limit.
     */
    int limit = -1;

    // -----

    /**
     * The DataTable we are deleting from .
     */
    private DataTable update_table;

    /**
     * The TableName object of the table being created.
     */
    private TableName tname;

    /**
     * Tables that are relationally linked to the table being inserted into, set
     * after 'prepare'.  This is used to determine the tables we need to read
     * lock because we need to validate relational constraints on the tables.
     */
    private ArrayList relationally_linked_tables;

    /**
     * The plan for the set of records we are deleting in this query.
     */
    private QueryPlanNode plan;


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {

        // Get variables from the model.
        table_name = (String) cmd.getObject("table_name");
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

        // Check that this user has privs to delete from the table.
        if (!database.getDatabase().canUserDeleteFromTableObject(context,
                user, tname)) {
            throw new UserAccessException(
                    "User not permitted to delete from table: " + table_name);
        }

        // Check the user has select permissions on the tables in the plan.
        Select.checkUserSelectPermissions(context, user, plan);

        // Evaluates the delete statement...

        // Evaluate the plan to find the update set.
        Table delete_set = plan.evaluate(context);

        // Delete from the data table.
        int delete_count = update_table.delete(delete_set, limit);

        // Notify TriggerManager that we've just done an update.
        if (delete_count > 0) {
            database.notifyTriggerEvent(new TriggerEvent(
                    TriggerEvent.DELETE, tname.toString(), delete_count));
        }

        // Return the number of columns we deleted.
        return FunctionTable.resultTable(context, delete_count);

    }


}
