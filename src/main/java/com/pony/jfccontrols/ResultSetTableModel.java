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

package com.pony.jfccontrols;

import javax.swing.table.*;
import java.sql.*;

/**
 * An implementation of a javax.swing.table.TableModel that updates itself from
 * a scrollable java.sql.ResultSet source.  This directly maps columns from a
 * query to columns in the table model.  If you wish to filter information
 * from the result set before it is output as a table use
 * FilteredResultSetTableModel.
 *
 * @author Tobias Downer
 */

public class ResultSetTableModel extends AbstractTableModel {

    /**
     * The scrollable ResultSet source.
     */
    private ResultSet result_set;

    /**
     * The ResultSetMetaData object for this result set.
     */
    private ResultSetMetaData meta_data;

    /**
     * The number of rows in the result set.
     */
    private int row_count;

    /**
     * If true, a table structure change event is NOT thrown if the result set
     * looks similar to an updated result set.
     */
    private boolean preserve_table_structure;


    /**
     * Constructs the model.
     */
    public ResultSetTableModel(ResultSet result_set) {
        super();
        preserve_table_structure = false;
        if (result_set != null) {
            updateResultSet(result_set);
        } else {
            clear();
        }
    }

    public ResultSetTableModel() {
        this(null);
    }

    /**
     * A property that checks for changes when a result set is updated and
     * preserves the layout if the updated result set looks similar.  This means
     * that the width of columns in the UI will not change to their default
     * values.
     */
    public void setPreserveTableStructure(boolean status) {
        preserve_table_structure = status;
    }

    /**
     * Updates the result set in this model with the given ResultSet object.
     */
    public void updateResultSet(ResultSet result_set) {
        try {
            boolean fire_structure_change = true;
            if (this.result_set != null) {
                // If the 'preserve_table_structure' flag is set, we want to check if
                // there are any changes between the old meta data structure and the
                // new one.
                if (preserve_table_structure) {
                    // Did the result set change?
                    ResultSetMetaData old_meta_data = this.meta_data;
                    ResultSetMetaData new_meta_data = result_set.getMetaData();
                    int col_count = new_meta_data.getColumnCount();
                    if (old_meta_data.getColumnCount() == col_count) {
                        boolean different = false;
                        for (int i = 1; i < col_count + 1 && !different; ++i) {
                            different =
                                    (!old_meta_data.getColumnName(i).equals(
                                            new_meta_data.getColumnName(i)) ||
                                            !old_meta_data.getTableName(i).equals(
                                                    new_meta_data.getTableName(i)) ||
                                            !old_meta_data.getSchemaName(i).equals(
                                                    new_meta_data.getSchemaName(i)));
                        }
                        fire_structure_change = different;
                    }
                }
                this.result_set.close();
            }

            // Set up the new result set info.
            this.result_set = result_set;
            this.meta_data = result_set.getMetaData();

            // Move to the end of the result set and get the row index.  This is a
            // fast way to work out the row count, however it won't work efficiently
            // in many database systems.
            if (result_set.last()) {
                row_count = result_set.getRow();
            } else {
                row_count = 0;
            }

            if (fire_structure_change) {
                fireTableStructureChanged();
            } else {
                fireTableDataChanged();
            }

        } catch (SQLException e) {
            throw new Error("SQL Exception: " + e.getMessage());
        }
    }

    /**
     * Clears the model of the current result set.
     */
    public void clear() {
        // Close the old result set if needed.
        if (result_set != null) {
            try {
                result_set.close();
            } catch (SQLException e) {
                // Just incase the JDBC driver can't close a result set twice.
                e.printStackTrace();
            }
        }
        result_set = null;
        meta_data = null;
        row_count = 0;
        fireTableStructureChanged();
    }

    // ---------- Implemented from AbstractTableModel ----------

    public int getRowCount() {
        return row_count;
    }

    public int getColumnCount() {
        if (meta_data != null) {
            try {
                return meta_data.getColumnCount();
            } catch (SQLException e) {
                throw new Error("SQL Exception: " + e.getMessage());
            }
        }
        return 0;
    }

    public String getColumnName(int column) {
        if (meta_data != null) {
            try {
                return meta_data.getColumnLabel(column + 1);
            } catch (SQLException e) {
                throw new Error("SQL Exception: " + e.getMessage());
            }
        }
        throw new Error("No columns!");
    }

    public Object getValueAt(int row, int column) {
        if (result_set != null) {
            try {
                result_set.absolute(row + 1);
                return result_set.getObject(column + 1);
            } catch (SQLException e) {
                throw new Error("SQL Exception: " + e.getMessage());
            }
        }
        throw new Error("No contents!");
    }

}
