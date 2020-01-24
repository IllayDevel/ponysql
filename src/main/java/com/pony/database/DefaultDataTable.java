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

import com.pony.util.IntegerVector;

/**
 * This represents a default implementation of a DataTable.  It encapsulates
 * information that is core to all DataTable objects.  That is,
 * <p>
 *   The table name,
 *   The description of the table fields,
 *   A set of SelectableScheme objects to describe row relations,
 *   A counter for the number of rows in the table.
 * <p>
 * There are two classes that extend this object.  DataTable which is a
 * DataTable that is a direct mapping to an internal table stored in the
 * Database files.  And TemporaryTable that contains information generated
 * on the fly by the DBMS.
 * <p>
 * @author Tobias Downer
 */

public abstract class DefaultDataTable extends AbstractDataTable {

    /**
     * The Database object that this table is a child of.
     */
    private final Database database;

    /**
     * The number of rows in the table.
     */
    protected int row_count;

    /**
     * A list of schemes for managing the data relations of each column.
     */
    private SelectableScheme[] column_scheme;

    /**
     * The Constructor.
     */
    DefaultDataTable(Database database) {
        super();
        this.database = database;
        this.row_count = 0;
    }

    /**
     * Returns the Database object this table is part of.
     */
    public Database getDatabase() {
        return database;
    }

    /**
     * Returns the SelectableScheme for the given column.  This is different from
     * 'getColumnScheme(int column)' because this is designed to be overridden
     * so derived classes can manage their own SelectableScheme sources.
     */
    protected SelectableScheme getRootColumnScheme(int column) {
        return column_scheme[column];
    }

    /**
     * Clears the SelectableScheme information for the given column.
     */
    protected void clearColumnScheme(int column) {
        column_scheme[column] = null;
    }

    /**
     * Blanks all the column schemes in the table to an initial state.  This
     * will make all schemes of type InsertSearch.
     * <p>
     * <strong>NOTE:</strong>
     *   The current default SelectableScheme type is InsertSearch.  We may want
     *   to make this variable.
     */
    protected void blankSelectableSchemes() {
        blankSelectableSchemes(0);
    }

    /**
     * Blanks all the column schemes in this table to a specific type of
     * scheme.  If Type = 0 then InsertSearch (fast but takes up memory -
     * requires each insert and delete from the table to be logged).  If type
     * = 1 then BlindSearch (slower but uses no memory and doesn't require
     * insert and delete to be logged).
     */
    protected void blankSelectableSchemes(int type) {
        column_scheme = new SelectableScheme[getColumnCount()];
        for (int i = 0; i < column_scheme.length; ++i) {
            if (type == 0) {
                column_scheme[i] = new InsertSearch(this, i);
            } else if (type == 1) {
                column_scheme[i] = new BlindSearch(this, i);
            }
        }
    }

    /**
     * Returns the number of columns in the table.
     */
    public int getColumnCount() {
        return getDataTableDef().columnCount();
    }

    /**
     * Returns the number of rows stored in the table.
     */
    public int getRowCount() {
        return row_count;
    }

//  /**
//   * Returns a list of all the fields within the table.  The list is ordered
//   * the same way the fields were added in to the table.  NOTE: if you use the
//   * TableField.getName() method, it will not be fully resolved.  There will
//   * be no information about the table the field came from in the object.
//   */
//  public TableField[] getFields() {
//    System.out.println("NOTE: Calls to DefaultDataTable.getFields() need to be deprecated.");
//    return getDataTableDef().toTableFieldArray();
//  }

//  /**
//   * Returns the field at the given column.  Note the the name of the field
//   * will not be fully resolved.  It contains to information about the table
//   * the field came from.
//   */
//  public TableField getFieldAt(int column) {
//    System.out.println("NOTE: Calls to DefaultDataTable.getFieldAt() need to be deprecated.");
//    return getDataTableDef().columnAt(column).tableFieldValue();
//  }

//  /**
//   * Returns the fully resolved name of the given column in this table.
//   *
//   * @deprecated
//   */
//  public String getResolvedColumnName(int column) {
//    StringBuffer out = new StringBuffer();
//    out.append(getTableName().getName());
//    out.append('.');
//    out.append(getFieldAt(column).getName());
//    return new String(out);
//  }

    /**
     * Returns a fully qualified Variable object that represents the name of
     * the column at the given index.  For example,
     *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
     */
    public Variable getResolvedVariable(int column) {
        String col_name = getDataTableDef().columnAt(column).getName();
        return new Variable(getTableName(), col_name);
    }

//  /**
//   * Given a field name, ie. 'CUSTOMER.CUSTOMERID' this will return the
//   * column number the field is at.  Note that this method requires that the
//   * type of the column (ie. the Table) be appended to the start.  Returns
//   * -1 if the field could not be found in the table.
//   *
//   * @deprecated
//   */
//  public int findFieldName(String name) {
//    int point_index = name.indexOf('.');
//    if (point_index == -1) {
//      throw new Error("Can't find '.' deliminator in name: " + name);
//    }
//    String type = name.substring(0, point_index);
//    String col_name = name.substring(point_index + 1);
//
//    if (type.equals(getName())) {
//      int size = getColumnCount();
//      for (int i = 0; i < size; ++i) {
//        TableField field = getFieldAt(i);
//        if (field.getName().equals(col_name)) {
//          return i;
//        }
//      }
//    }
//
//    return -1;
//  }

    /**
     * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
     * this will return the column number the field is at.  Returns -1 if the
     * field does not exist in the table.
     */
    public int findFieldName(Variable v) {
        // Check this is the correct table first...
        TableName table_name = v.getTableName();
        DataTableDef table_def = getDataTableDef();
        if (table_name != null && table_name.equals(getTableName())) {
            // Look for the column name
            String col_name = v.getName();
            int size = getColumnCount();
            for (int i = 0; i < size; ++i) {
                DataTableColumnDef col = table_def.columnAt(i);
                if (col.getName().equals(col_name)) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Returns a SelectableScheme object for the given column of the
     * VirtualTable.  The Table parameter specifies the domain in which the
     * scheme should be given.  If table != this, we can safely assume it is a
     * VirtualTable.
     */
    SelectableScheme getSelectableSchemeFor(int column, int original_column,
                                            Table table) {
        SelectableScheme scheme = getRootColumnScheme(column);

//    System.out.println("DefaultDataTable.getSelectableSchemaFor(" +
//                       column + ", " + original_column + ", " + table);

//    System.out.println(this);

        // If we are getting a scheme for this table, simple return the information
        // from the column_trees Vector.
        if (table == this) {
            return scheme;
        }

        // Otherwise, get the scheme to calculate a subset of the given scheme.
        else {
            return scheme.getSubsetScheme(table, original_column);
        }

    }

    /**
     * Given a set, this trickles down through the Table hierarchy resolving
     * the given row_set to a form that the given ancestor understands.
     * Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
     * down three levels and return a new 7 element set with the rows fully
     * resolved to the given ancestors domain.
     */
    void setToRowTableDomain(int column, IntegerVector row_set,
                             TableDataSource ancestor) {
        if (ancestor != this) {
            throw new RuntimeException("Method routed to incorrect table ancestor.");
        }
    }

    /**
     * Return the list of DataTable and row sets that make up the raw information
     * in this table.  For a DataTable itselt, this is trivial.
     * NOTE: Using this method is extremely inefficient, and should never be
     * used.  It is included only to complete feature set.
     * IDEA: Put a warning to check if this method is ever used.
     */
    RawTableInformation resolveToRawTable(RawTableInformation info) {
        System.err.println("Efficiency Warning in DataTable.resolveToRawTable.");
        IntegerVector row_set = new IntegerVector();
        RowEnumeration e = rowEnumeration();
        while (e.hasMoreRows()) {
            row_set.addInt(e.nextRowIndex());
        }
        info.add(this, row_set);
        return info;
    }

//  /**
//   * Returns a bit vector indicating the columns that are valid.
//   */
//  boolean[] validColumns() {
//    int len = getColumnCount();
//    boolean[] bit_vector = new boolean[len];
//    for (int i = 0; i < len; ++i) {
//      bit_vector[i] = true;
//    }
//    return bit_vector;
//  }

  /* ===== Convenience methods for updating internal information =====
     =============== regarding the SelectableSchemes ================= */

    /**
     * Adds a single column of a row to the selectable scheme indexing.
     */
    void addCellToColumnSchemes(int row_number, int column_number) {
        boolean indexable_type =
                getDataTableDef().columnAt(column_number).isIndexableType();
        if (indexable_type) {
            SelectableScheme ss = getRootColumnScheme(column_number);
            ss.insert(row_number);
        }
    }

    /**
     * This is called when a row is in the table, and the SelectableScheme
     * objects for each column need to be notified of the rows existance,
     * therefore build up the relational model for the columns.
     */
    void addRowToColumnSchemes(int row_number) {
        int col_count = getColumnCount();
        DataTableDef table_def = getDataTableDef();
        for (int i = 0; i < col_count; ++i) {
            if (table_def.columnAt(i).isIndexableType()) {
                SelectableScheme ss = getRootColumnScheme(i);
                ss.insert(row_number);
            }
        }
    }

    /**
     * This is called when an index to a row needs to be removed from the
     * SelectableScheme objects.  This occurs when we have a modification log
     * of row removals that haven't actually happened to old backed up scheme.
     */
    void removeRowToColumnSchemes(int row_number) {
        int col_count = getColumnCount();
        DataTableDef table_def = getDataTableDef();
        for (int i = 0; i < col_count; ++i) {
            if (table_def.columnAt(i).isIndexableType()) {
                SelectableScheme ss = getRootColumnScheme(i);
                ss.remove(row_number);
            }
        }
    }

}
