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

/**
 * An implementation of FromTableInterface that wraps around a
 * TableSelectExpression object as a sub-query source.
 *
 * @author Tobias Downer
 */

public class FromTableSubQuerySource implements FromTableInterface {

    /**
     * The wrapped object.
     */
    private TableSelectExpression table_expression;

    /**
     * The fully prepared TableExpressionFromSet object that is used to
     * qualify variables in the table.
     */
    private TableExpressionFromSet from_set;

    /**
     * The TableName that this source is generated to (aliased name).  If null,
     * we inherit from the root set.
     */
    private TableName end_table_name;

    /**
     * A unique name given to this source that is used to reference it in a
     * TableSet.
     */
    private String unique_key;

    /**
     * The list of all variable names in the resultant source.
     */
    private Variable[] vars;

    /**
     * Set to true if this should do case insensitive resolutions.
     */
    private boolean case_insensitive = false;

    /**
     * Constructs the source.
     */
    public FromTableSubQuerySource(DatabaseConnection connection,
                                   String unique_key,
                                   TableSelectExpression table_expression,
                                   TableExpressionFromSet from_set,
                                   TableName aliased_table_name) {
        this.unique_key = unique_key;
        this.table_expression = table_expression;
        this.from_set = from_set;
        this.end_table_name = aliased_table_name;
        // Is the database case insensitive?
        this.case_insensitive = connection.isInCaseInsensitiveMode();
    }

    /**
     * Returns the TableSelectExpression for this sub-query.
     */
    TableSelectExpression getTableExpression() {
        return table_expression;
    }

    /**
     * Returns the TableExpressionFromSet for this sub-query.
     */
    TableExpressionFromSet getFromSet() {
        return from_set;
    }

    /**
     * Returns the aliased table name of this sub-query or null if it is left
     * as-is.
     */
    TableName getAliasedName() {
        return end_table_name;
    }


    /**
     * Makes sure the 'vars' list is created correctly.
     */
    private void ensureVarList() {
        if (vars == null) {
            vars = from_set.generateResolvedVariableList();
//      for (int i = 0; i < vars.length; ++i) {
//        System.out.println("+ " + vars[i]);
//      }
//      System.out.println("0000");
            // Are the variables aliased to a table name?
            if (end_table_name != null) {
                for (int i = 0; i < vars.length; ++i) {
                    vars[i].setTableName(end_table_name);
                }
            }
        }
    }

    /**
     * Returns the unique name of this source.
     */
    public String getUniqueKey() {
        return unique_key;
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

    /**
     * If the given Variable matches the reference then this method returns
     * true.
     */
    private boolean matchesVar(Variable v, String catalog, String schema,
                               String table, String column) {
        TableName tn = v.getTableName();
        String cn = v.getName();

        if (column == null) {
            return true;
        }
        if (!stringCompare(cn, column)) {
            return false;
        }

        if (table == null) {
            return true;
        }
        if (tn == null) {
            return false;
        }
        String tname = tn.getName();
        if (tname != null && !stringCompare(tname, table)) {
            return false;
        }

        if (schema == null) {
            return true;
        }
        String sname = tn.getSchema();
        return sname == null || stringCompare(sname, schema);

        // Currently we ignore catalog

    }

    // ---------- Implemented from FromTableInterface ----------

    public String getUniqueName() {
        return getUniqueKey();
    }

    public boolean matchesReference(String catalog,
                                    String schema, String table) {
        if (schema == null && table == null) {
            return true;
        }
        if (end_table_name != null) {
            String ts = end_table_name.getSchema();
            String tt = end_table_name.getName();
            if (schema == null) {
                return stringCompare(tt, table);
            } else {
                return stringCompare(tt, table) && stringCompare(ts, schema);
            }
        }
        // No way to determine if there is a match
        return false;
    }

    public int resolveColumnCount(String catalog, String schema,
                                  String table, String column) {
        ensureVarList();

        if (catalog == null && schema == null && table == null && column == null) {
            // Return the column count
            return vars.length;
        }

        int matched_count = 0;
        for (int i = 0; i < vars.length; ++i) {
            Variable v = vars[i];
            if (matchesVar(v, catalog, schema, table, column)) {
                ++matched_count;
            }
        }

        return matched_count;

    }

    public Variable resolveColumn(String catalog, String schema,
                                  String table, String column) {
        ensureVarList();

//    System.out.println("resolveColumn: " + catalog + ", " + schema + ", " +
//                       table + ", " + column);

        for (int i = 0; i < vars.length; ++i) {
            Variable v = vars[i];
            if (matchesVar(v, catalog, schema, table, column)) {
//        System.out.println("Result: " + v);
                return v;
            }
        }

        throw new Error("Couldn't resolve to a column.");
    }

    public Variable[] allColumns() {
        ensureVarList();
        return vars;
    }

}
