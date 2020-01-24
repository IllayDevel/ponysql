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

package com.pony.database.interpret;

import com.pony.database.*;

import java.util.ArrayList;

/**
 * Logic for the ALTER TABLE SQL statement.
 *
 * @author Tobias Downer
 */

public class AlterTable extends Statement {

    /**
     * The create statement that we use to alter the current table.  This is
     * only for compatibility reasons.
     */
    StatementTree create_statement;

    /**
     * The name of the table we are altering.
     */
    String table_name;

    /**
     * The list of actions to perform in this alter statement.
     */
    private ArrayList<Object> actions;

    /**
     * The TableName object.
     */
    private TableName tname;

    /**
     * The prepared create table statement.
     */
    CreateTable create_stmt;


    /**
     * Adds an action to perform in this alter statement.
     */
    public void addAction(AlterTableAction action) {
        if (actions == null) {
            actions = new ArrayList<>();
        }
        actions.add(action);
    }

    /**
     * Returns true if the column names match.  If the database is in case
     * insensitive mode then the columns will match if the case insensitive
     * search matches.
     */
    public boolean checkColumnNamesMatch(DatabaseConnection db,
                                         String col1, String col2) {
        if (db.isInCaseInsensitiveMode()) {
            return col1.equalsIgnoreCase(col2);
        }
        return col1.equals(col2);
    }

    private void checkColumnConstraint(String col_name, String[] cols,
                                       TableName table, String constraint_name) {
        for (String col : cols) {
            if (col_name.equals(col)) {
                throw new DatabaseConstraintViolationException(
                        DatabaseConstraintViolationException.DROP_COLUMN_VIOLATION,
                        "Constraint violation (" + constraint_name +
                                ") dropping column " + col_name + " because of " +
                                "referential constraint in " + table);
            }
        }

    }


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {

        // Get variables from the model
        table_name = (String) cmd.getObject("table_name");
        addAction((AlterTableAction) cmd.getObject("alter_action"));
        create_statement = (StatementTree) cmd.getObject("create_statement");

        // ---

        if (create_statement != null) {
            create_stmt = new CreateTable();
            create_stmt.init(database, create_statement, null);
            create_stmt.prepare();
            this.table_name = create_stmt.table_name;
//      create_statement.doPrepare(db, user);
        } else {
            // If we don't have a create statement, then this is an SQL alter
            // command.
        }

//    tname = TableName.resolve(db.getCurrentSchema(), table_name);
        tname = resolveTableName(table_name, database);
        if (tname.getName().indexOf('.') != -1) {
            throw new DatabaseException("Table name can not contain '.' character.");
        }

    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        String schema_name = database.getCurrentSchema();

        // Does the user have privs to alter this tables?
        if (!database.getDatabase().canUserAlterTableObject(context, user, tname)) {
            throw new UserAccessException(
                    "User not permitted to alter table: " + table_name);
        }

        if (create_statement != null) {

            // Create the data table definition and tell the database to update it.
            DataTableDef table_def = create_stmt.createDataTableDef();
            TableName tname = table_def.getTableName();
            // Is the table in the database already?
            if (database.tableExists(tname)) {
                // Drop any schema for this table,
                database.dropAllConstraintsForTable(tname);
                database.updateTable(table_def);
            }
            // If the table isn't in the database,
            else {
                database.createTable(table_def);
            }

            // Setup the constraints
            create_stmt.setupAllConstraints();

            // Return '0' if we created the table.
            return FunctionTable.resultTable(context, 0);
        } else {
            // SQL alter command using the alter table actions,

            // Get the table definition for the table name,
            DataTableDef table_def = database.getTable(tname).getDataTableDef();
            String table_name = table_def.getName();
            DataTableDef new_table = table_def.noColumnCopy();

            // Returns a ColumnChecker implementation for this table.
            ColumnChecker checker =
                    ColumnChecker.standardColumnChecker(database, tname);

            // Set to true if the table topology is alter, or false if only
            // the constraints are changed.
            boolean table_altered = false;

            for (int n = 0; n < table_def.columnCount(); ++n) {
                DataTableColumnDef column =
                        new DataTableColumnDef(table_def.columnAt(n));
                String col_name = column.getName();
                // Apply any actions to this column
                boolean mark_dropped = false;
                for (Object o : actions) {
                    AlterTableAction action = (AlterTableAction) o;
                    if (action.getAction().equals("ALTERSET") &&
                            checkColumnNamesMatch(database,
                                    (String) action.getElement(0),
                                    col_name)) {
                        Expression exp = (Expression) action.getElement(1);
                        checker.checkExpression(exp);
                        column.setDefaultExpression(exp);
                        table_altered = true;
                    } else if (action.getAction().equals("DROPDEFAULT") &&
                            checkColumnNamesMatch(database,
                                    (String) action.getElement(0),
                                    col_name)) {
                        column.setDefaultExpression(null);
                        table_altered = true;
                    } else if (action.getAction().equals("DROP") &&
                            checkColumnNamesMatch(database,
                                    (String) action.getElement(0),
                                    col_name)) {
                        // Check there are no referential links to this column
                        Transaction.ColumnGroupReference[] refs =
                                database.queryTableImportedForeignKeyReferences(tname);
                        for (Transaction.ColumnGroupReference columnGroupReference : refs) {
                            checkColumnConstraint(col_name, columnGroupReference.ref_columns,
                                    columnGroupReference.ref_table_name, columnGroupReference.name);
                        }
                        // Or from it
                        refs = database.queryTableForeignKeyReferences(tname);
                        for (Transaction.ColumnGroupReference ref : refs) {
                            checkColumnConstraint(col_name, ref.key_columns,
                                    ref.key_table_name, ref.name);
                        }
                        // Or that it's part of a primary key
                        Transaction.ColumnGroup primary_key =
                                database.queryTablePrimaryKeyGroup(tname);
                        if (primary_key != null) {
                            checkColumnConstraint(col_name, primary_key.columns,
                                    tname, primary_key.name);
                        }
                        // Or that it's part of a unique set
                        Transaction.ColumnGroup[] uniques =
                                database.queryTableUniqueGroups(tname);
                        for (Transaction.ColumnGroup unique : uniques) {
                            checkColumnConstraint(col_name, unique.columns,
                                    tname, unique.name);
                        }

                        mark_dropped = true;
                        table_altered = true;
                    }
                }
                // If not dropped then add to the new table definition.
                if (!mark_dropped) {
                    new_table.addColumn(column);
                }
            }

            // Add any new columns,
            for (Object item : actions) {
                AlterTableAction action = (AlterTableAction) item;
                if (action.getAction().equals("ADD")) {
                    ColumnDef cdef = (ColumnDef) action.getElement(0);
                    if (cdef.isUnique() || cdef.isPrimaryKey()) {
                        throw new DatabaseException("Can not use UNIQUE or PRIMARY KEY " +
                                "column constraint when altering a column.  Use " +
                                "ADD CONSTRAINT instead.");
                    }
                    // Convert to a DataTableColumnDef
                    DataTableColumnDef col = CreateTable.convertColumnDef(cdef);

                    checker.checkExpression(
                            col.getDefaultExpression(database.getSystem()));
                    String col_name = col.getName();
                    // If column name starts with [table_name]. then strip it off
                    col.setName(ColumnChecker.stripTableName(table_name, col_name));
                    new_table.addColumn(col);
                    table_altered = true;
                }
            }

            // Any constraints to drop...
            for (Object value : actions) {
                AlterTableAction action = (AlterTableAction) value;
                if (action.getAction().equals("DROP_CONSTRAINT")) {
                    String constraint_name = (String) action.getElement(0);
                    int drop_count = database.dropNamedConstraint(tname, constraint_name);
                    if (drop_count == 0) {
                        throw new DatabaseException(
                                "Named constraint to drop on table " + tname +
                                        " was not found: " + constraint_name);
                    }
                } else if (action.getAction().equals("DROP_CONSTRAINT_PRIMARY_KEY")) {
                    boolean constraint_dropped =
                            database.dropPrimaryKeyConstraintForTable(tname, null);
                    if (!constraint_dropped) {
                        throw new DatabaseException(
                                "No primary key to delete on table " + tname);
                    }
                }
            }

            // Any constraints to add...
            for (Object o : actions) {
                AlterTableAction action = (AlterTableAction) o;
                if (action.getAction().equals("ADD_CONSTRAINT")) {
                    ConstraintDef constraint = (ConstraintDef) action.getElement(0);
                    boolean foreign_constraint =
                            (constraint.type == ConstraintDef.FOREIGN_KEY);
                    TableName ref_tname = null;
                    if (foreign_constraint) {
                        ref_tname =
                                resolveTableName(constraint.reference_table_name, database);
                        if (database.isInCaseInsensitiveMode()) {
                            ref_tname = database.tryResolveCase(ref_tname);
                        }
                        constraint.reference_table_name = ref_tname.toString();
                    }

                    ColumnChecker.stripColumnList(table_name, constraint.column_list);
                    ColumnChecker.stripColumnList(constraint.reference_table_name,
                            constraint.column_list2);
                    checker.checkExpression(constraint.check_expression);
                    checker.checkColumnList(constraint.column_list);
                    if (foreign_constraint && constraint.column_list2 != null) {
                        ColumnChecker referenced_checker =
                                ColumnChecker.standardColumnChecker(database, ref_tname);
                        referenced_checker.checkColumnList(constraint.column_list2);
                    }

                    CreateTable.addSchemaConstraint(database, tname, constraint);

                }
            }

            // Alter the existing table to the new format...
            if (table_altered) {
                if (new_table.columnCount() == 0) {
                    throw new DatabaseException(
                            "Can not ALTER table to have 0 columns.");
                }
                database.updateTable(new_table);
            } else {
                // If the table wasn't physically altered, check the constraints.
                // Calling this method will also make the transaction check all
                // deferred constraints during the next commit.
                database.checkAllConstraints(tname);
            }

            // Return '0' if everything successful.
            return FunctionTable.resultTable(context, 0);

        }

    }


}
