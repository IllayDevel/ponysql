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

import com.pony.database.global.Types;
import com.pony.database.global.StringObject;

import java.util.List;
import java.util.Arrays;

/**
 * Represents a row of data to be added into a table.  The row data is linked
 * to a TableField that describes the cell information within a row.
 * <p>
 * There are two types of RowData object.  Those that are empty and contain
 * blank data, and those that contain information to either be inserted
 * into a table, or has be retrieved from a row.
 * <p>
 * NOTE: Any RowData objects that need to be set to 'null' should be done so
 *   explicitly.
 * NOTE: We must call a 'setColumnData' method for _every_ column in the
 *   row to form.
 * NOTE: This method (or derived classes) must only use safe methods in
 *   DataTable.  (ie. getRowCount, etc are out).
 *
 * @author Tobias Downer
 */

public class RowData implements Types {

    /**
     * The TransactionSystem this RowData is a context of.
     */
    private final TransactionSystem system;

    /**
     * The TableDataSource object that this RowData is in, or is destined to be
     * in.
     */
    private TableDataSource table;

    /**
     * The definition of the table.
     */
    private DataTableDef table_def;

    /**
     * A list of TObject objects in the table.
     */
    private final TObject[] data_cell_list;

    /**
     * The number of columns in the row.
     */
    private final int col_count;


    /**
     * To create a RowData object without an underlying table.  This is for
     * copying from one table to a different one.
     */
    public RowData(TransactionSystem system, int col_count) {
        this.system = system;
        this.col_count = col_count;
        data_cell_list = new TObject[col_count];
    }

    /**
     * The Constructor generates a blank row.
     */
    public RowData(TableDataSource table) {
        this.system = table.getSystem();
        this.table = table;
        table_def = table.getDataTableDef();
        col_count = table_def.columnCount();
        data_cell_list = new TObject[col_count];
    }

    /**
     * Populates the RowData object with information from a specific row from
     * the underlying DataTable.
     */
    void setFromRow(int row) {
        for (int col = 0; col < col_count; ++col) {
            setColumnData(col, table.getCellContents(col, row));
        }
    }

    /**
     * Returns the table object this row data is assigned to.  This is used to
     * ensure we don't try to use a row data in a different table to what it was
     * created from.
     */
    boolean isSameTable(DataTable tab) {
        return table == tab;
    }

    /**
     * Sets up a column by casting the value from the given TObject to a
     * type that is compatible with the column.  This is useful when we
     * are copying information from one table to another.
     */
    public void setColumnData(int column, TObject cell) {
        DataTableColumnDef col = table_def.columnAt(column);
        if (table != null && col.getSQLType() != cell.getTType().getSQLType()) {
            // Cast the TObject
            cell = cell.castTo(col.getTType());
        }
        setColumnDataFromTObject(column, cell);
    }

    /**
     * Sets up a column from an Object.
     */
    public void setColumnDataFromObject(int column, Object ob) {
        DataTableColumnDef col_def = table_def.columnAt(column);

        if (ob instanceof String) {
            ob = StringObject.fromString((String) ob);
        }

        // Create a TObject from the given object to the given type
        TObject cell = TObject.createAndCastFromObject(col_def.getTType(), ob);
        setColumnDataFromTObject(column, cell);
    }

    /**
     * Sets up a column from a TObject.
     */
    public void setColumnDataFromTObject(int column, TObject ob) {
        data_cell_list[column] = ob;
    }

    /**
     * This is a special case situation for setting the column cell to 'null'.
     */
    public void setColumnToNull(int column) {
        DataTableColumnDef col_def = table_def.columnAt(column);
        setColumnDataFromTObject(column, new TObject(col_def.getTType(), null));
    }

    /**
     * Sets the given column number to the default value for this column.
     */
    public void setColumnToDefault(int column, QueryContext context) {
        if (table != null) {
            DataTableColumnDef column_def = table_def.columnAt(column);
            Expression exp = column_def.getDefaultExpression(system);
            if (exp != null) {
                TObject def_val = evaluate(exp, context);
                setColumnData(column, def_val);
                return;
            }
        }
        setColumnToNull(column);
    }

    /**
     * Returns the TObject that represents the information in the given column
     * of the row.
     */
    public TObject getCellData(int column) {
        TObject cell = data_cell_list[column];
        if (cell == null) {
            DataTableColumnDef col_def = table_def.columnAt(column);
            cell = new TObject(col_def.getTType(), null);
        }
        return cell;
    }

    /**
     * Returns the name of the given column number.
     */
    public String getColumnName(int column) {
        return table_def.columnAt(column).getName();
    }

    /**
     * Finds the field in this RowData with the given name.
     */
    public int findFieldName(String column_name) {
        return table_def.findColumnName(column_name);
    }

    /**
     * Returns the number of columns (cells) in this row.
     */
    public int getColumnCount() {
        return col_count;
    }

    /**
     * Evaluates the expression and returns the object it evaluates to using
     * the local VariableResolver to resolve variables in the expression.
     */
    TObject evaluate(Expression expression, QueryContext context) {
        boolean ignore_case = system.ignoreIdentifierCase();
        // Resolve any variables to the table_def for this expression.
        table_def.resolveColumns(ignore_case, expression);
        // Get the variable resolver and evaluate over this data.
        VariableResolver vresolver = getVariableResolver();
        return expression.evaluate(null, vresolver, context);
    }

    /**
     * Evaluates a single assignment on this RowData object.  A VariableResolver
     * is made which resolves to variables only within this RowData context.
     */
    void evaluate(Assignment assignment, QueryContext context) {

        // Get the variable resolver and evaluate over this data.
        VariableResolver vresolver = getVariableResolver();
        TObject ob = assignment.getExpression().evaluate(null, vresolver, context);

        // Check the variable name is within this row.
        Variable variable = assignment.getVariable();
        int column = findFieldName(variable.getName());

        // Set the column to the resolved value.
        setColumnData(column, ob);
    }

    /**
     * Any columns in the row of data that haven't been set yet (they will be
     * 'null') will be set to the default value during this method.  This should
     * be called after the row data has initially been set with values from some
     * source.
     */
    public void setDefaultForRest(QueryContext context)
            throws DatabaseException {
        for (int i = 0; i < col_count; ++i) {
            if (data_cell_list[i] == null) {
                setColumnToDefault(i, context);
            }
        }
    }

    /**
     * Sets up an entire row given the array of assignments.  If any columns are
     * left 'null' then they are filled with the default value.
     */
    public void setupEntire(Assignment[] assignments, QueryContext context)
            throws DatabaseException {
        for (Assignment assignment : assignments) {
            evaluate(assignment, context);
        }
        // Any that are left as 'null', set to default value.
        setDefaultForRest(context);
    }

    /**
     * Sets up an entire row given the list of insert elements and a list of
     * indices to the columns to set.  An insert element is either an expression
     * that is resolved to a constant, or the string "DEFAULT" which indicates
     * the value should be set to the default value of the column.
     */
    public void setupEntire(int[] col_indices, List insert_elements,
                            QueryContext context) throws DatabaseException {
        int elem_size = insert_elements.size();
        if (col_indices.length != elem_size) {
            throw new DatabaseException(
                    "Column indices and expression array sizes don't match");
        }
        // Get the variable resolver and evaluate over this data.
        VariableResolver vresolver = getVariableResolver();
        for (int i = 0; i < col_indices.length; ++i) {
            Object element = insert_elements.get(i);
            if (element instanceof Expression) {
                // Evaluate to the object to insert
                TObject ob = ((Expression) element).evaluate(null, vresolver, context);
                int table_column = col_indices[i];
                // Cast the object to the type of the column
                ob = ob.castTo(table_def.columnAt(table_column).getTType());
                // Set the column to the resolved value.
                setColumnDataFromTObject(table_column, ob);
            } else {
                // The element must be 'DEFAULT'.  If it's not throw an error.  If it
                // is, the default value will be set later.
                if (!element.equals("DEFAULT")) {
                    throw new DatabaseException(
                            "Invalid value in 'insert_elements' list.");
                }
            }
        }
        // Any that are left as 'null', set to default value.
        setDefaultForRest(context);
    }

    /**
     * Sets up an entire row given the array of Expressions and a list of indices
     * to the columns to set.  Any columns that are not set by this method are
     * set to the default value as defined for the column.
     */
    public void setupEntire(int[] col_indices, Expression[] exps,
                            QueryContext context) throws DatabaseException {
        setupEntire(col_indices, Arrays.asList(exps), context);
    }

    /**
     * Returns a string representation of this row.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[RowData: ");
        for (int i = 0; i < col_count; ++i) {
            buf.append(data_cell_list[i].getObject());
            buf.append(", ");
        }
        return new String(buf);
    }

    /**
     * Returns a VariableResolver to use within this RowData context.
     */
    private VariableResolver getVariableResolver() {
        if (variable_resolver == null) {
            variable_resolver = new RDVariableResolver();
        } else {
            variable_resolver.nextAssignment();
        }
        return variable_resolver;

    }

    private RDVariableResolver variable_resolver = null;

    // ---------- Inner classes ----------

    /**
     * Variable resolver for this context.
     */
    private class RDVariableResolver implements VariableResolver {

        private int assignment_count = 0;

        void nextAssignment() {
            ++assignment_count;
        }

        public int setID() {
            return assignment_count;
        }

        public TObject resolve(Variable variable) {
            String col_name = variable.getName();

            int col_index = table_def.findColumnName(col_name);
            if (col_index == -1) {
                throw new Error("Can't find column: " + col_name);
            }

            TObject cell = data_cell_list[col_index];

            if (cell == null) {
                throw new Error("Column " + col_name + " hasn't been set yet.");
            }

            return cell;
        }

        public TType returnTType(Variable variable) {
            String col_name = variable.getName();

            int col_index = table_def.findColumnName(col_name);
            if (col_index == -1) {
                throw new Error("Can't find column: " + col_name);
            }

            return table_def.columnAt(col_index).getTType();
        }

    }

}
