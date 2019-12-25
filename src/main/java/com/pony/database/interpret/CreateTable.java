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
 * A parsed state container for the 'create' statement.
 *
 * @author Tobias Downer
 */

public class CreateTable extends Statement {

    /**
     * Set to true if this create statement is for a temporary table.
     */
    boolean temporary = false;

    /**
     * Only create if table doesn't exist.
     */
    boolean only_if_not_exists = false;

    /**
     * The name of the table to create.
     */
    String table_name;

    /**
     * List of column declarations (ColumnDef)
     */
    ArrayList columns;

    /**
     * List of table constraints (ConstraintDef)
     */
    ArrayList constraints;

//  /**
//   * The expression that must be evaluated to true for this row to be
//   * added to the table.
//   */
//  Expression check_exp;

    /**
     * The TableName object.
     */
    private TableName tname;


//  /**
//   * Adds a new ColumnDef object to this create statement.  A ColumnDef
//   * object describes a column for the new table we are creating.  The column's
//   * must be added in the order they are to be in the created table.
//   */
//  void addColumnDef(ColumnDef column) {
//    columns.addElement(column);
//  }

    /**
     * Adds a new ConstraintDef object to this create statement.  A ConstraintDef
     * object describes any constraints for the new table we are creating.
     */
    void addConstraintDef(ConstraintDef constraint) {
        constraints.add(constraint);
    }

//  /**
//   * Handles the create statement 'CHECK' expression for compatibility.
//   */
//  void addCheckConstraint(Expression check_expression) {
//    ConstraintDef constraint = new ConstraintDef();
//    constraint.setCheck(check_expression);
//    constraints.addElement(constraint);
//  }

    /**
     * Creates a DataTableDef that describes the table that was defined by
     * this create statement.  This is used by the 'alter' statement.
     */
    DataTableDef createDataTableDef() throws DatabaseException {
        // Make all this information into a DataTableDef object...
        DataTableDef table_def = new DataTableDef();
        table_def.setTableName(tname);
        table_def.setTableClass("com.pony.database.VariableSizeDataTableFile");

        // Add the columns.
        // NOTE: Any duplicate column names will be found here...
        for (Object column : columns) {
            DataTableColumnDef cd = (DataTableColumnDef) column;
            table_def.addColumn(cd);
        }

        return table_def;
    }


    /**
     * Adds a schema constraint to the rules for the schema represented by the
     * manager.
     */
    static void addSchemaConstraint(DatabaseConnection manager,
                                    TableName table, ConstraintDef constraint)
            throws DatabaseException {
        if (constraint.type == ConstraintDef.PRIMARY_KEY) {
            manager.addPrimaryKeyConstraint(table,
                    constraint.getColumnList(), constraint.deferred, constraint.name);
        } else if (constraint.type == ConstraintDef.FOREIGN_KEY) {
            // Currently we forbid referencing a table in another schema
            TableName ref_table =
                    TableName.resolve(constraint.reference_table_name);
            String update_rule = constraint.getUpdateRule().toUpperCase();
            String delete_rule = constraint.getDeleteRule().toUpperCase();
            if (table.getSchema().equals(ref_table.getSchema())) {
                manager.addForeignKeyConstraint(
                        table, constraint.getColumnList(),
                        ref_table, constraint.getColumnList2(),
                        delete_rule, update_rule, constraint.deferred, constraint.name);
            } else {
                throw new DatabaseException("Foreign key reference error: " +
                        "Not permitted to reference a table outside of the schema: " +
                        table + " -> " + ref_table);
            }
        } else if (constraint.type == ConstraintDef.UNIQUE) {
            manager.addUniqueConstraint(table, constraint.getColumnList(),
                    constraint.deferred, constraint.name);
        } else if (constraint.type == ConstraintDef.CHECK) {
            manager.addCheckConstraint(table, constraint.original_check_expression,
                    constraint.deferred, constraint.name);
        } else {
            throw new DatabaseException("Unrecognized constraint type.");
        }
    }

    /**
     * Returns a com.pony.database.interpret.ColumnDef object a a
     * com.pony.database.DataTableColumnDef object.
     */
    static DataTableColumnDef convertColumnDef(ColumnDef cdef) {
        TType type = cdef.type;

        DataTableColumnDef dtcdef = new DataTableColumnDef();
        dtcdef.setName(cdef.name);
        dtcdef.setNotNull(cdef.isNotNull());
        dtcdef.setFromTType(type);

        if (cdef.index_str != null) {
            dtcdef.setIndexScheme(cdef.index_str);
        }
        if (cdef.default_expression != null) {
            dtcdef.setDefaultExpression(cdef.original_default_expression);
        }

        dtcdef.initTTypeInfo();
        return dtcdef;
    }

    /**
     * Sets up all constraints specified in this create statement.
     */
    void setupAllConstraints() throws DatabaseException {
        for (Object o : constraints) {
            ConstraintDef constraint = (ConstraintDef) o;

            // Add this to the schema manager tables
            addSchemaConstraint(database, tname, constraint);
        }
    }


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {

        // Get the state from the model
        temporary = cmd.getBoolean("temporary");
        only_if_not_exists = cmd.getBoolean("only_if_not_exists");
        table_name = (String) cmd.getObject("table_name");
        ArrayList column_list = (ArrayList) cmd.getObject("column_list");
        constraints = (ArrayList) cmd.getObject("constraint_list");

        // Convert column_list to list of com.pony.database.DataTableColumnDef
        int size = column_list.size();
        columns = new ArrayList(size);
        for (Object value : column_list) {
            ColumnDef cdef = (ColumnDef) value;
            columns.add(convertColumnDef(cdef));
        }

        // ----

        String schema_name = database.getCurrentSchema();
        tname = TableName.resolve(schema_name, table_name);

        String name_strip = tname.getName();

        if (name_strip.indexOf('.') != -1) {
            throw new DatabaseException("Table name can not contain '.' character.");
        }

        final boolean ignores_case = database.isInCaseInsensitiveMode();

        // Implement the checker class for this statement.
        ColumnChecker checker = new ColumnChecker() {

            String resolveColumnName(String col_name) throws DatabaseException {
                // We need to do case sensitive and case insensitive resolution,
                String found_col = null;
                for (Object column : columns) {
                    DataTableColumnDef col = (DataTableColumnDef) column;
                    if (!ignores_case) {
                        if (col.getName().equals(col_name)) {
                            return col_name;
                        }
                    } else {
                        if (col.getName().equalsIgnoreCase(col_name)) {
                            if (found_col != null) {
                                throw new DatabaseException("Ambiguous column name '" +
                                        col_name + "'");
                            }
                            found_col = col.getName();
                        }
                    }
                }
                return found_col;
            }

        };

        ArrayList unique_column_list = new ArrayList();
        ArrayList primary_key_column_list = new ArrayList();

        // Check the expressions that represent the default values for the columns.
        // Also check each column name
        for (int i = 0; i < columns.size(); ++i) {
            DataTableColumnDef cdef = (DataTableColumnDef) columns.get(i);
            ColumnDef model_cdef = (ColumnDef) column_list.get(i);
            checker.checkExpression(cdef.getDefaultExpression(database.getSystem()));
            String col_name = cdef.getName();
            // If column name starts with [table_name]. then strip it off
            cdef.setName(ColumnChecker.stripTableName(name_strip, col_name));
            // If unique then add to unique columns
            if (model_cdef.isUnique()) {
                unique_column_list.add(col_name);
            }
            // If primary key then add to primary key columns
            if (model_cdef.isPrimaryKey()) {
                primary_key_column_list.add(col_name);
            }
        }

        // Add the unique and primary key constraints.
        if (unique_column_list.size() > 0) {
            ConstraintDef constraint = new ConstraintDef();
            constraint.setUnique(unique_column_list);
            addConstraintDef(constraint);
        }
        if (primary_key_column_list.size() > 0) {
            ConstraintDef constraint = new ConstraintDef();
            constraint.setPrimaryKey(primary_key_column_list);
            addConstraintDef(constraint);
        }

        // Strip the column names and set the expression in all the constraints.
        for (Object o : constraints) {
            ConstraintDef constraint = (ConstraintDef) o;
            ColumnChecker.stripColumnList(name_strip, constraint.column_list);
            // Check the referencing table for foreign keys
            if (constraint.type == ConstraintDef.FOREIGN_KEY) {
                ColumnChecker.stripColumnList(constraint.reference_table_name,
                        constraint.column_list2);
                TableName ref_tname =
                        resolveTableName(constraint.reference_table_name, database);
                if (database.isInCaseInsensitiveMode()) {
                    ref_tname = database.tryResolveCase(ref_tname);
                }
                constraint.reference_table_name = ref_tname.toString();

                DataTableDef ref_table_def;
                if (database.tableExists(ref_tname)) {
                    // Get the DataTableDef for the table we are referencing
                    ref_table_def = database.getDataTableDef(ref_tname);
                } else if (ref_tname.equals(tname)) {
                    // We are referencing the table we are creating
                    ref_table_def = createDataTableDef();
                } else {
                    throw new DatabaseException(
                            "Referenced table '" + ref_tname + "' in constraint '" +
                                    constraint.name + "' does not exist.");
                }
                // Resolve columns against the given table def
                ref_table_def.resolveColumnsInArray(database, constraint.column_list2);

            }
            checker.checkExpression(constraint.check_expression);
            checker.checkColumnList(constraint.column_list);
        }

    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        // Does the schema exist?
        boolean ignore_case = database.isInCaseInsensitiveMode();
        SchemaDef schema =
                database.resolveSchemaCase(tname.getSchema(), ignore_case);
        if (schema == null) {
            throw new DatabaseException("Schema '" + tname.getSchema() +
                    "' doesn't exist.");
        } else {
            tname = new TableName(schema.getName(), tname.getName());
        }

        // Does the user have privs to create this tables?
        if (!database.getDatabase().canUserCreateTableObject(context,
                user, tname)) {
            throw new UserAccessException(
                    "User not permitted to create table: " + table_name);
        }


        // PENDING: Creation of temporary tables...


        // Does the table already exist?
        if (!database.tableExists(tname)) {

            // Create the data table definition and tell the database to create
            // it.
            DataTableDef table_def = createDataTableDef();
            database.createTable(table_def);

            // The initial grants for a table is to give the user who created it
            // full access.
            database.getGrantManager().addGrant(
                    Privileges.TABLE_ALL_PRIVS, GrantManager.TABLE, tname.toString(),
                    user.getUserName(), true, Database.INTERNAL_SECURE_USERNAME);

            // Set the constraints in the schema.
            setupAllConstraints();

            // Return '0' if we created the table.  (0 rows affected)
            return FunctionTable.resultTable(context, 0);
        }

        // Report error unless 'if not exists' command is in the statement.
        if (only_if_not_exists == false) {
            throw new DatabaseException("Table '" + tname + "' already exists.");
        }

        // Return '0' (0 rows affected).  This happens when we don't create a
        // table (because it exists) and the 'IF NOT EXISTS' clause is present.
        return FunctionTable.resultTable(context, 0);

    }

}
