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

import java.util.ArrayList;

/**
 * Handler for creating and dropping views in the database.
 *
 * @author Tobias Downer
 */

public class ViewManager extends Statement {

    /**
     * The type of command we are running through this ViewManager.
     */
    private String type;

    /**
     * The view name to create/drop.
     */
    private String view_name;

    /**
     * The view name as a TableName object.
     */
    private TableName vname;

    /**
     * If this is a create command, the QueryPlanNode that represents the view
     * plan.
     */
    private QueryPlanNode plan;


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        type = (String) cmd.getObject("type");
        view_name = (String) cmd.getObject("view_name");

        String schema_name = database.getCurrentSchema();
        vname = TableName.resolve(schema_name, view_name);
        vname = database.tryResolveCase(vname);

        if (type.equals("create")) {
            // Get the select expression
            /**
             * If this is a create command, the TableSelectExpression that forms the view.
             */
            TableSelectExpression select_expression = (TableSelectExpression) cmd.getObject("select_expression");
            // Get the column name list
            ArrayList col_list = (ArrayList) cmd.getObject("column_list");

            // Generate the TableExpressionFromSet hierarchy for the expression,
            TableExpressionFromSet from_set =
                    Planner.generateFromSet(select_expression, database);
            // Form the plan
            plan = Planner.formQueryPlan(database, select_expression, from_set,
                    new ArrayList());

            // Wrap the result around a SubsetNode to alias the columns in the
            // table correctly for this view.
            int sz = (col_list == null) ? 0 : col_list.size();
            Variable[] original_vars = from_set.generateResolvedVariableList();
            Variable[] new_column_vars = new Variable[original_vars.length];

            if (sz > 0) {
                if (sz != original_vars.length) {
                    throw new StatementException(
                            "Column list is not the same size as the columns selected.");
                }
                for (int i = 0; i < sz; ++i) {
                    String col_name = (String) col_list.get(i);
                    new_column_vars[i] = new Variable(vname, col_name);
                }
            } else {
                sz = original_vars.length;
                for (int i = 0; i < sz; ++i) {
                    new_column_vars[i] = new Variable(vname, original_vars[i].getName());
                }
            }

            // Check there are no repeat column names in the table.
            for (int i = 0; i < sz; ++i) {
                Variable cur_v = new_column_vars[i];
                for (int n = i + 1; n < sz; ++n) {
                    if (new_column_vars[n].equals(cur_v)) {
                        throw new DatabaseException(
                                "Duplicate column name '" + cur_v + "' in view.  " +
                                        "A view may not contain duplicate column names.");
                    }
                }
            }

            // Wrap the plan around a SubsetNode plan
            plan = new QueryPlan.SubsetNode(plan, original_vars, new_column_vars);

        }

    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        if (type.equals("create")) {
            // Does the user have privs to create this tables?
            if (!database.getDatabase().canUserCreateTableObject(context,
                    user, vname)) {
                throw new UserAccessException(
                        "User not permitted to create view: " + view_name);
            }

            // Does the schema exist?
            boolean ignore_case = database.isInCaseInsensitiveMode();
            SchemaDef schema =
                    database.resolveSchemaCase(vname.getSchema(), ignore_case);
            if (schema == null) {
                throw new DatabaseException("Schema '" + vname.getSchema() +
                        "' doesn't exist.");
            } else {
                vname = new TableName(schema.getName(), vname.getName());
            }

            // Check the permissions for this user to select from the tables in the
            // given plan.
            Select.checkUserSelectPermissions(context, user, plan);

            // Does the table already exist?
            if (database.tableExists(vname)) {
                throw new DatabaseException("View or table with name '" + vname +
                        "' already exists.");
            }

            // Before evaluation, make a clone of the plan,
            QueryPlanNode plan_copy;
            try {
                plan_copy = (QueryPlanNode) plan.clone();
            } catch (CloneNotSupportedException e) {
                Debug().writeException(e);
                throw new DatabaseException("Clone error: " + e.getMessage());
            }

            // We have to execute the plan to get the DataTableDef that represents the
            // result of the view execution.
            Table t = plan.evaluate(context);
            DataTableDef data_table_def = new DataTableDef(t.getDataTableDef());
            data_table_def.setTableName(vname);

            // Create a ViewDef object,
            ViewDef view_def = new ViewDef(data_table_def, plan_copy);

            // And create the view object,
            database.createView(query, view_def);

            // The initial grants for a view is to give the user who created it
            // full access.
            database.getGrantManager().addGrant(
                    Privileges.TABLE_ALL_PRIVS, GrantManager.TABLE, vname.toString(),
                    user.getUserName(), true, Database.INTERNAL_SECURE_USERNAME);

        } else if (type.equals("drop")) {

            // Does the user have privs to drop this tables?
            if (!database.getDatabase().canUserDropTableObject(context,
                    user, vname)) {
                throw new UserAccessException(
                        "User not permitted to drop view: " + view_name);
            }

            // Drop the view object
            database.dropView(vname);

            // Drop the grants for this object
            database.getGrantManager().revokeAllGrantsOnObject(
                    GrantManager.TABLE, vname.toString());

        } else {
            throw new Error("Unknown view command type: " + type);
        }

        return FunctionTable.resultTable(context, 0);
    }


}

