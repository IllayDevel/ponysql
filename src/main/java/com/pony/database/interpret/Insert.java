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
import com.pony.util.IntegerVector;

/**
 * The instance class that stores all the information about an insert
 * statement for processing.
 *
 * @author Tobias Downer
 */

public class Insert extends Statement {

    String table_name;

    ArrayList col_list;

    ArrayList values_list;    //list contains List of elements to insert

    StatementTree select;

    ArrayList column_sets;

    boolean from_values = false;

    boolean from_select = false;

    boolean from_set = false;

    // -----

    /**
     * The table we are inserting stuff to.
     */
    private DataTable insert_table;

    /**
     * For 'from_values' and 'from_select', this is a list of indices into the
     * 'insert_table' for the columns that we are inserting data into.
     */
    private int[] col_index_list;

    /**
     * The list of Variable objects the represent the list of columns being
     * inserted into in this query.
     */
    private Variable[] col_var_list;

    /**
     * The TableName we are inserting into.
     */
    private TableName tname;

    /**
     * If this is a 'from_select' insert, the prepared Select object.
     */
    private Select prepared_select;


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {

        // Prepare this object from the StatementTree
        table_name = (String) cmd.getObject("table_name");
        col_list = (ArrayList) cmd.getObject("col_list");
        values_list = (ArrayList) cmd.getObject("data_list");
        select = (StatementTree) cmd.getObject("select");
        column_sets = (ArrayList) cmd.getObject("assignments");
        String type = (String) cmd.getObject("type");
        from_values = type.equals("from_values");
        from_select = type.equals("from_select");
        from_set = type.equals("from_set");

        // ---

        // Check 'values_list' contains all same size size insert element arrays.
        int first_len = -1;
        for (int n = 0; n < values_list.size(); ++n) {
            List exp_list = (List) values_list.get(n);
            if (first_len == -1 || first_len == exp_list.size()) {
                first_len = exp_list.size();
            } else {
                throw new DatabaseException("The insert data list varies in size.");
            }
        }

        tname = resolveTableName(table_name, database);

        // Does the table exist?
        if (!database.tableExists(tname)) {
            throw new DatabaseException("Table '" + tname + "' does not exist.");
        }

        // Add the from table direct source for this table
        TableQueryDef table_query_def = database.getTableQueryDef(tname, null);
        addTable(new FromTableDirectSource(database,
                table_query_def, "INSERT_TABLE", tname, tname));

        // Get the table we are inserting to
        insert_table = database.getTable(tname);

        // If column list is empty, then fill it with all columns from table.
        if (from_values || from_select) {
            // If 'col_list' is empty we must pick every entry from the insert
            // table.
            if (col_list.size() == 0) {
                for (int i = 0; i < insert_table.getColumnCount(); ++i) {
                    col_list.add(insert_table.getColumnDefAt(i).getName());
                }
            }
            // Resolve 'col_list' into a list of column indices into the insert
            // table.
            col_index_list = new int[col_list.size()];
            col_var_list = new Variable[col_list.size()];
            for (int i = 0; i < col_list.size(); ++i) {
//        Variable col = Variable.resolve(tname, (String) col_list.get(i));
                Variable in_var = Variable.resolve((String) col_list.get(i));
                Variable col = resolveColumn(in_var);
                int index = insert_table.fastFindFieldName(col);
                if (index == -1) {
                    throw new DatabaseException("Can't find column: " + col);
                }
                col_index_list[i] = index;
                col_var_list[i] = col;
            }
        }

        // Make the 'from_values' clause into a 'from_set'
        if (from_values) {

            // If values to insert is different from columns list,
            if (col_list.size() != ((List) values_list.get(0)).size()) {
                throw new DatabaseException("Number of columns to insert is " +
                        "different from columns selected to insert to.");
            }

            // Resolve all expressions in the added list.
            // For each value
            for (int i = 0; i < values_list.size(); ++i) {
                // Each value is a list of either expressions or "DEFAULT"
                List insert_elements = (List) values_list.get(i);
                int sz = insert_elements.size();
                for (int n = 0; n < sz; ++n) {
                    Object elem = insert_elements.get(n);
                    if (elem instanceof Expression) {
                        Expression exp = (Expression) elem;
                        List elem_list = exp.allElements();
                        for (int p = 0; p < elem_list.size(); ++p) {
                            Object ob = elem_list.get(p);
                            if (ob instanceof Select) {
                                throw new DatabaseException(
                                        "Illegal to have sub-select in expression.");
                            }
                        }
                        // Resolve the expression.
                        resolveExpression(exp);
                    }
                }
            }

        } else if (from_select) {
            // Prepare the select statement
            prepared_select = new Select();
            prepared_select.init(database, select, null);
            prepared_select.prepare();
        }

        // If from a set, then resolve all values,
        else if (from_set) {

            // If there's a sub select in an expression in the 'SET' clause then
            // throw an error.
            for (int i = 0; i < column_sets.size(); ++i) {
                Assignment assignment = (Assignment) column_sets.get(i);
                Expression exp = assignment.getExpression();
                List elem_list = exp.allElements();
                for (int n = 0; n < elem_list.size(); ++n) {
                    Object ob = elem_list.get(n);
                    if (ob instanceof Select) {
                        throw new DatabaseException(
                                "Illegal to have sub-select in SET clause.");
                    }
                }

                // Resolve the column names in the columns set.
                Variable v = assignment.getVariable();
                Variable resolved_v = resolveVariableName(v);
                v.set(resolved_v);
                resolveExpression(assignment.getExpression());
            }

        }

        // Resolve all tables linked to this
        TableName[] linked_tables =
                database.queryTablesRelationallyLinkedTo(tname);
        ArrayList relationally_linked_tables = new ArrayList(linked_tables.length);
        for (int i = 0; i < linked_tables.length; ++i) {
            relationally_linked_tables.add(database.getTable(linked_tables[i]));
        }

    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        // Check that this user has privs to insert into the table.
        if (!database.getDatabase().canUserInsertIntoTableObject(
                context, user, tname, col_var_list)) {
            throw new UserAccessException(
                    "User not permitted to insert in to table: " + table_name);
        }

        // Are we inserting from a select statement or from a 'set' assignment
        // list?
        int insert_count = 0;

        if (from_values) {
            // Set each row from the VALUES table,
            for (int i = 0; i < values_list.size(); ++i) {
                List insert_elements = (List) values_list.get(i);
                RowData row_data = insert_table.createRowDataObject(context);
                row_data.setupEntire(col_index_list, insert_elements, context);
                insert_table.add(row_data);
                ++insert_count;
            }
        } else if (from_select) {
            // Insert rows from the result select table.
            Table result = prepared_select.evaluate();
            if (result.getColumnCount() != col_index_list.length) {
                throw new DatabaseException(
                        "Number of columns in result don't match columns to insert.");
            }

            // Copy row list into an intermediate IntegerVector list.
            // (A RowEnumeration for a table being modified is undefined).
            IntegerVector row_list = new IntegerVector();
            RowEnumeration en = result.rowEnumeration();
            while (en.hasMoreRows()) {
                row_list.addInt(en.nextRowIndex());
            }

            // For each row of the select table.
            int sz = row_list.size();
            for (int i = 0; i < sz; ++i) {
                int rindex = row_list.intAt(i);
                RowData row_data = insert_table.createRowDataObject(context);
                for (int n = 0; n < col_index_list.length; ++n) {
                    TObject cell = result.getCellContents(n, rindex);
                    row_data.setColumnData(col_index_list[n], cell);
                }
                row_data.setDefaultForRest(context);
                insert_table.add(row_data);
                ++insert_count;
            }
        } else if (from_set) {
            // Insert rows from the set assignments.
            RowData row_data = insert_table.createRowDataObject(context);
            Assignment[] assignments = (Assignment[])
                    column_sets.toArray(new Assignment[column_sets.size()]);
            row_data.setupEntire(assignments, context);
            insert_table.add(row_data);
            ++insert_count;
        }

        // Notify TriggerManager that we've just done an update.
        if (insert_count > 0) {
            database.notifyTriggerEvent(new TriggerEvent(
                    TriggerEvent.INSERT, tname.toString(), insert_count));
        }

        // Return the number of rows we inserted.
        return FunctionTable.resultTable(context, insert_count);
    }

}
