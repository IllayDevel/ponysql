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

import java.util.List;
import java.util.Collections;

/**
 * An implementation of FromTableInterface that wraps around an
 * TableName/AbstractDataTable object.  The handles case insensitive
 * resolution.
 *
 * @author Tobias Downer
 */

public class FromTableDirectSource implements FromTableInterface {

    /**
     * The TableQueryDef object that links to the underlying table.
     */
    private TableQueryDef table_query;

    /**
     * The DataTableDef object that describes the table.
     */
    private DataTableDef data_table_def;

    /**
     * The unique name given to this source.
     */
    private String unique_name;

    /**
     * The given TableName of this table.
     */
    private TableName table_name;

    /**
     * The root name of the table.  For example, if this table is 'Part P' the
     * root name is 'Part' and 'P' is the aliased name.
     */
    private TableName root_name;

    /**
     * Set to true if this should do case insensitive resolutions.
     */
    private boolean case_insensitive = false;

    /**
     * Constructs the source.
     */
    public FromTableDirectSource(DatabaseConnection connection,
                                 TableQueryDef table_query, String unique_name,
                                 TableName given_name, TableName root_name) {
        this.unique_name = unique_name;
        this.data_table_def = table_query.getDataTableDef();
        this.root_name = root_name;
        if (given_name != null) {
            this.table_name = given_name;
        } else {
            this.table_name = root_name;
        }
        // Is the database case insensitive?
        this.case_insensitive = connection.isInCaseInsensitiveMode();
        this.table_query = table_query;
    }

    /**
     * Returns the given name of the table.  For example, if the Part table is
     * aliased as P this returns P.  If there is no given name, returns the
     * root table name.
     */
    public TableName getGivenTableName() {
        return table_name;
    }

    /**
     * Returns the root name of the table.  This TableName can always be used as
     * a direct reference to a table in the database.
     */
    public TableName getRootTableName() {
        return root_name;
    }

    /**
     * Creates a QueryPlanNode to be added into a query tree that fetches the
     * table source.
     */
    public QueryPlanNode createFetchQueryPlanNode() {
        return table_query.getQueryPlanNode();
    }

    /**
     * Toggle the case sensitivity flag.
     */
    public void setCaseInsensitive(boolean status) {
        case_insensitive = status;
    }

    private boolean stringCompare(String str1, String str2) {
        if (!case_insensitive) {
            return str1.equals(str2);
        }
        return str1.equalsIgnoreCase(str2);
    }


    // ---------- Implemented from FromTableInterface ----------

    public String getUniqueName() {
        return unique_name;
    }

    public boolean matchesReference(String catalog,
                                    String schema, String table) {
//    System.out.println("Matches reference: " + schema + " " + table);
//    System.out.println(table_name.getName());

        // Does this table name represent the correct schema?
        if (schema != null &&
                !stringCompare(schema, table_name.getSchema())) {
            // If schema is present and we can't resolve to this schema then false
            return false;
        }
        // If table name is present and we can't resolve to this table name
        // then return false
        return table == null ||
                stringCompare(table, table_name.getName());
//    System.out.println("MATCHED!");
        // Match was successful,
    }

    public int resolveColumnCount(String catalog, String schema,
                                  String table, String column) {
        // NOTE: With this type, we can only ever return either 1 or 0 because
        //   it's impossible to have an ambiguous reference

        // NOTE: Currently 'catalog' is ignored.

        // Does this table name represent the correct schema?
        if (schema != null &&
                !stringCompare(schema, table_name.getSchema())) {
            // If schema is present and we can't resolve to this schema then return 0
            return 0;
        }
        if (table != null &&
                !stringCompare(table, table_name.getName())) {
            // If table name is present and we can't resolve to this table name then
            // return 0
            return 0;
        }

        if (column != null) {
            if (!case_insensitive) {
                // Can we resolve the column in this table?
                int i = data_table_def.fastFindColumnName(column);
                // If i doesn't equal -1 then we've found our column
                return i == -1 ? 0 : 1;
            } else {
                // Case insensitive search (this is slower than case sensitive).
                int resolve_count = 0;
                int col_count = data_table_def.columnCount();
                for (int i = 0; i < col_count; ++i) {
                    if (data_table_def.columnAt(i).getName().equalsIgnoreCase(column)) {
                        ++resolve_count;
                    }
                }
                return resolve_count;
            }
        } else {  // if (column == null)
            // Return the column count
            return data_table_def.columnCount();
        }
    }

    public Variable resolveColumn(String catalog, String schema,
                                  String table, String column) {

        // Does this table name represent the correct schema?
        if (schema != null &&
                !stringCompare(schema, table_name.getSchema())) {
            // If schema is present and we can't resolve to this schema
            throw new Error("Incorrect schema.");
        }
        if (table != null &&
                !stringCompare(table, table_name.getName())) {
            // If table name is present and we can't resolve to this table name
            throw new Error("Incorrect table.");
        }

        if (column != null) {
            if (!case_insensitive) {
                // Can we resolve the column in this table?
                int i = data_table_def.fastFindColumnName(column);
                if (i == -1) {
                    throw new Error("Could not resolve '" + column + "'");
                }
                return new Variable(table_name, column);
            } else {
                // Case insensitive search (this is slower than case sensitive).
                int col_count = data_table_def.columnCount();
                for (int i = 0; i < col_count; ++i) {
                    String col_name = data_table_def.columnAt(i).getName();
                    if (col_name.equalsIgnoreCase(column)) {
                        return new Variable(table_name, col_name);
                    }
                }
                throw new Error("Could not resolve '" + column + "'");
            }
        } else {  // if (column == null)
            // Return the first column in the table
            return new Variable(table_name, data_table_def.columnAt(0).getName());
        }

    }

    public Variable[] allColumns() {
        int col_count = data_table_def.columnCount();
        Variable[] vars = new Variable[col_count];
        for (int i = 0; i < col_count; ++i) {
            vars[i] = new Variable(table_name, data_table_def.columnAt(i).getName());
        }
        return vars;
    }

}
