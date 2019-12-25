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
import java.util.List;

/**
 * A class that abstracts the checking of information in a table.  This is
 * abstracted because the behaviour is shared between ALTER and CREATE
 * statement.
 *
 * @author Tobias Downer
 */

abstract class ColumnChecker {

    /**
     * Given a column name string, this will strip off the preceeding table
     * name if there is one specified.  For example, 'Customer.id' would
     * become 'id'.  This also checks that the table specification is in the
     * given table domain.  For example,
     * stripTableName("Customer", "Customer.id") would not throw an error but
     * stripTableName("Order", "Customer.di") would.
     */
    static String stripTableName(String table_domain, String column) {
        if (column.indexOf('.') != -1) {
            String st = table_domain + ".";
            if (!column.startsWith(st)) {
                throw new StatementException("Column '" + column +
                        "' is not within the expected table domain '" + table_domain + "'");
            }
            column = column.substring(st.length());
        }
        return column;
    }

    /**
     * Calls the 'stripTableName' method on all elements in the given list.
     */
    static ArrayList stripColumnList(String table_domain,
                                     ArrayList column_list) {
        if (column_list != null) {
            int size = column_list.size();
            for (int i = 0; i < size; ++i) {
                String res = stripTableName(table_domain, (String) column_list.get(i));
                column_list.set(i, res);
            }
        }
        return column_list;
    }

    /**
     * Returns the resolved column name if the column exists within the table
     * being checked under, or null if it doesn't.  Throws an error if the
     * column name is abiguous.
     */
    abstract String resolveColumnName(String col_name) throws DatabaseException;

    /**
     * Resolves all the variables in the expression throwing a DatabaseException
     * if any errors found.  This checks that all variables point to a column
     * in the table being created.
     */
    void checkExpression(Expression expression) throws DatabaseException {

        if (expression != null) {
            List list = expression.allVariables();
            for (int i = 0; i < list.size(); ++i) {
                Variable v = (Variable) list.get(i);
                String orig_col = v.getName();
                String resolved_column = resolveColumnName(orig_col);
                if (resolved_column == null) {
                    throw new DatabaseException("Column '" + orig_col +
                            "' not found in the table.");
                }
                // Resolve the column name
                if (!orig_col.equals(resolved_column)) {
                    v.setColumnName(resolved_column);
                }

            }

            // Don't allow select statements because they don't convert to a
            // text string that we can encode into the DataTableDef file.
            if (expression.hasSubQuery()) {
                throw new DatabaseException("Sub-queries not permitted in " +
                        "the check constraint expression.");
            }
        }

    }

    /**
     * Checks all the columns in the list and throws an exception if any
     * column names are not found in the columns in this create.  Additionally
     * sets the entry with the correct column resolved to.
     */
    void checkColumnList(ArrayList list) throws DatabaseException {
        if (list != null) {
            for (int i = 0; i < list.size(); ++i) {
                String col = (String) list.get(i);
                String resolved_col = resolveColumnName(col);
                if (resolved_col == null) {
                    throw new DatabaseException(
                            "Column '" + col + "' not found the table.");
                }
                list.set(i, resolved_col);
            }
        }
    }


    // ---------- Statics ----------

    /**
     * Given a DatabaseConnection and a TableName object, this returns an
     * implementation of ColumnChecker that is able to check that the column
     * name exists in the table, and that the reference is not ambigious.
     */
    static ColumnChecker standardColumnChecker(
            DatabaseConnection database, TableName tname) {
        final DataTableDef table_def = database.getTable(tname).getDataTableDef();
        final boolean ignores_case = database.isInCaseInsensitiveMode();

        // Implement the checker
        return new ColumnChecker() {
            String resolveColumnName(String col_name) throws DatabaseException {
                // We need to do case sensitive and case insensitive resolution,
                String found_col = null;
                for (int n = 0; n < table_def.columnCount(); ++n) {
                    DataTableColumnDef col =
                            (DataTableColumnDef) table_def.columnAt(n);
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
    }

}
