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

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A definition of a table.  Every table in the database has a definition
 * that describes how it is stored on disk, the column definitions, primary
 * keys/foreign keys, and any check constraints.
 *
 * @author Tobias Downer
 */

public class DataTableDef {

    /**
     * A TableName object that represents this data table def.
     */
    private TableName table_name;

    /**
     * The type of table this is (this is the class name of the object that
     * maintains the underlying database files).
     */
    private String table_type_class;

    /**
     * The list of DataTableColumnDef objects that are the definitions of each
     * column in the table.
     */
    private final ArrayList column_list;


    /**
     * Set to true if this data table def is immutable.
     */
    private boolean immutable;

    /**
     * Constructs this DataTableDef file.
     */
    public DataTableDef() {
        column_list = new ArrayList();
        table_type_class = "";
        immutable = false;
    }

    /**
     * Copy constructor.
     */
    public DataTableDef(DataTableDef table_def) {
        table_name = table_def.getTableName();
        table_type_class = table_def.table_type_class;
        column_list = (ArrayList) table_def.column_list.clone();

        // Copy is not immutable
        immutable = false;
    }

    /**
     * Sets this DataTableDef to immutable which means nothing is able to
     * change it.
     */
    public void setImmutable() {
        immutable = true;
    }

    /**
     * Returns true if this is immutable.
     */
    public boolean immutable() {
        return immutable;
    }

    /**
     * Checks that this object is mutable.  If it isn't an exception is thrown.
     */
    private void checkMutable() {
        if (immutable()) {
            throw new Error("Tried to mutate immutable object.");
        }
    }

    /**
     * Outputs to the PrintStream for debugging.
     */
    public void dump(PrintStream out) {
        for (int i = 0; i < columnCount(); ++i) {
            columnAt(i).dump(out);
            out.println();
        }
    }

    /**
     * Resolves variables in a column so that any unresolved column names point
     * to this table.  Used to resolve columns in the 'check_expression'.
     */
    void resolveColumns(boolean ignore_case, Expression exp) {

        // For each variable, determine if the column correctly resolves to a
        // column in this table.  If the database is in identifier case insensitive
        // mode attempt to resolve the column name to a valid column in this
        // def.
        if (exp != null) {
            List list = exp.allVariables();
            for (Object o : list) {
                Variable v = (Variable) o;
                String col_name = v.getName();
                // Can we resolve this to a variable in the table?
                if (ignore_case) {
                    int size = columnCount();
                    for (int n = 0; n < size; ++n) {
                        // If this is a column name (case ignored) then set the variable
                        // to the correct cased name.
                        if (columnAt(n).getName().equalsIgnoreCase(col_name)) {
                            v.setColumnName(columnAt(n).getName());
                        }
                    }
                }
            }

        }
    }

    /**
     * Resolves a single column name to its correct form.  For example, if
     * the database is in case insensitive mode it'll resolve ID to 'id' if
     * 'id' is in this table.  Throws a database exception if a column couldn't
     * be resolved (ambiguous or not found).
     */
    public String resolveColumnName(String col_name, boolean ignore_case)
            throws DatabaseException {
        // Can we resolve this to a column in the table?
        int size = columnCount();
        int found = -1;
        for (int n = 0; n < size; ++n) {
            // If this is a column name (case ignored) then set the column
            // to the correct cased name.
            String this_col_name = columnAt(n).getName();
            if (ignore_case && this_col_name.equalsIgnoreCase(col_name)) {
                if (found == -1) {
                    found = n;
                } else {
                    throw new DatabaseException(
                            "Ambiguous reference to column '" + col_name + "'");
                }
            } else if (!ignore_case && this_col_name.equals(col_name)) {
                found = n;
            }
        }
        if (found != -1) {
            return columnAt(found).getName();
        } else {
            throw new DatabaseException("Column '" + col_name + "' not found");
        }
    }

    /**
     * Given a list of column names referencing entries in this table, this will
     * resolve each one to its correct form.  Throws a database exception if
     * a column couldn't be resolved.
     */
    public void resolveColumnsInArray(DatabaseConnection connection,
                                      ArrayList list) throws DatabaseException {
        boolean ignore_case = connection.isInCaseInsensitiveMode();
        for (int i = 0; i < list.size(); ++i) {
            String col_name = (String) list.get(i);
            list.set(i, resolveColumnName((String) list.get(i), ignore_case));
        }
    }

    // ---------- Set methods ----------

    public void setTableName(TableName name) {
        this.table_name = name;
    }

    public void setTableClass(String clazz) {
        checkMutable();
        if (clazz.equals("com.pony.database.VariableSizeDataTableFile")) {
            table_type_class = clazz;
        } else {
            throw new Error("Unrecognised table class: " + clazz);
        }
    }

    public void addColumn(DataTableColumnDef col_def) {
        checkMutable();
        // Is there already a column with this name in the table def?
        for (Object o : column_list) {
            DataTableColumnDef cd = (DataTableColumnDef) o;
            if (cd.getName().equals(col_def.getName())) {
                throw new Error("Duplicated columns found.");
            }
        }
        column_list.add(col_def);
    }

    /**
     * Same as 'addColumn' only this does not perform a check to ensure no
     * two columns are the same.
     */
    public void addVirtualColumn(DataTableColumnDef col_def) {
        checkMutable();
        column_list.add(col_def);
    }


    // ---------- Get methods ----------

    public String getSchema() {
        String schema_name = table_name.getSchema();
        return schema_name == null ? "" : schema_name;
    }

    public String getName() {
        return table_name.getName();
    }

    public TableName getTableName() {
        return table_name;
    }

    public String getTableClass() {
        return table_type_class;
    }

    public int columnCount() {
        return column_list.size();
    }

    public DataTableColumnDef columnAt(int column) {
        return (DataTableColumnDef) column_list.get(column);
    }

    public int findColumnName(String column_name) {
        int size = columnCount();
        for (int i = 0; i < size; ++i) {
            if (columnAt(i).getName().equals(column_name)) {
                return i;
            }
        }
        return -1;
    }

    // Stores col name -> col index lookups
    private transient HashMap col_name_lookup;
    private final transient Object COL_LOOKUP_LOCK = new Object();

    /**
     * A faster way to find a column index given a string column name.  This
     * caches column name -> column index in a HashMap.
     */
    public final int fastFindColumnName(String col) {
        synchronized (COL_LOOKUP_LOCK) {
            if (col_name_lookup == null) {
                col_name_lookup = new HashMap(30);
            }
            Object ob = col_name_lookup.get(col);
            if (ob == null) {
                int ci = findColumnName(col);
                col_name_lookup.put(col, ci);
                return ci;
            } else {
                return (Integer) ob;
            }
        }
    }


    /**
     * Returns a copy of this object, except with no columns or constraints.
     */
    public DataTableDef noColumnCopy() {
        DataTableDef def = new DataTableDef();
        def.setTableName(getTableName());
//    def.setSchema(schema);
//    def.setName(name);

        def.table_type_class = table_type_class;

        return def;
    }


    // ---------- In/Out methods ----------

    /**
     * Writes this DataTableDef file to the data output stream.
     */
    void write(DataOutput out) throws IOException {
        out.writeInt(2);  // Version number

        out.writeUTF(getName());
        out.writeUTF(getSchema());            // Added in version 2
        out.writeUTF(table_type_class);
        out.writeInt(column_list.size());
        for (Object o : column_list) {
            ((DataTableColumnDef) o).write(out);
        }

//    // -- Added in version 2 --
//    // Write the constraint list.
//    out.writeInt(constraint_list.size());
//    for (int i = 0; i < constraint_list.size(); ++i) {
//      ((DataTableConstraintDef) constraint_list.get(i)).write(out);
//    }

//    [ this is removed from version 1 ]
//    if (check_expression != null) {
//      out.writeBoolean(true);
//      // Write the text version of the expression to the stream.
//      out.writeUTF(new String(check_expression.text()));
//    }
//    else {
//      out.writeBoolean(false);
//    }

    }

    /**
     * Reads this DataTableDef file from the data input stream.
     */
    static DataTableDef read(DataInput in) throws IOException {
        DataTableDef dtf = new DataTableDef();
        int ver = in.readInt();
        if (ver == 1) {

            throw new IOException("Version 1 DataTableDef no longer supported.");

        } else if (ver == 2) {

            String rname = in.readUTF();
            String rschema = in.readUTF();
            dtf.setTableName(new TableName(rschema, rname));
            dtf.table_type_class = in.readUTF();
            int size = in.readInt();
            for (int i = 0; i < size; ++i) {
                DataTableColumnDef col_def = DataTableColumnDef.read(in);
                dtf.column_list.add(col_def);
            }

        } else {
            throw new Error("Unrecognized DataTableDef version (" + ver + ")");
        }

        dtf.setImmutable();
        return dtf;
    }

}
