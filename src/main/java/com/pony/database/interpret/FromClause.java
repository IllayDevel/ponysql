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
import java.util.Collection;

/**
 * A container for the From clause of a select statement.  This handles
 * the different types of joins.
 *
 * @author Tobias Downer
 */

public final class FromClause
        implements java.io.Serializable, StatementTreeObject, Cloneable {

    static final long serialVersionUID = 565726601314503609L;

    /**
     * The JoiningSet object that we have created to represent the joins in this
     * FROM clause.
     */
    private JoiningSet join_set = new JoiningSet();

    /**
     * A list of all FromTableDef objects in this clause in order of when they
     * were specified.
     */
    private ArrayList def_list = new ArrayList();

    /**
     * A list of all table names in this from clause.
     */
    private ArrayList all_table_names = new ArrayList();

    /**
     * An id used for making unique names for anonymous inner selects.
     */
    private int table_key = 0;


    /**
     * Creates a new unique key string.
     */
    private String createNewKey() {
        ++table_key;
        return Integer.toString(table_key);
    }


    private void addTableDef(String table_name, FromTableDef def) {
        if (table_name != null) {
            if (all_table_names.contains(table_name)) {
                throw new Error("Duplicate table name in FROM clause: " + table_name);
            }
            all_table_names.add(table_name);
        }
        // Create a new unique key for this table
        String key = createNewKey();
        def.setUniqueKey(key);
        // Add the table key to the join set
        join_set.addTable(new TableName(key));
        // Add to the alias def map
        def_list.add(def);
    }

    /**
     * Adds a table name to this FROM clause.  Note that the given name
     * may be a dot deliminated ref such as (schema.table_name).
     */
    public void addTable(String table_name) {
        addTableDef(table_name, new FromTableDef(table_name));
    }

    /**
     * Adds a table name + alias to this FROM clause.
     */
    public void addTable(String table_name, String table_alias) {
        addTableDef(table_alias, new FromTableDef(table_name, table_alias));
    }

    /**
     * A generic form of a table declaration.  If any parameters are 'null' it
     * means the information is not available.
     */
    public void addTableDeclaration(String table_name,
                                    TableSelectExpression select,
                                    String table_alias) {
        // This is an inner select in the FROM clause
        if (table_name == null && select != null) {
            if (table_alias == null) {
                addTableDef(null, new FromTableDef(select));
            } else {
                addTableDef(table_alias, new FromTableDef(select, table_alias));
            }
        }
        // This is a standard table reference in the FROM clause
        else if (table_name != null && select == null) {
            if (table_alias == null) {
                addTable(table_name);
            } else {
                addTable(table_name, table_alias);
            }
        }
        // Error
        else {
            throw new Error("Unvalid declaration parameters.");
        }

    }

    /**
     * Adds a Join to the from clause.  'type' must be a join type as defined
     * in JoiningSet.
     */
    public void addJoin(int type) {
//    System.out.println("Add Join: " + type);
        join_set.addJoin(type);
    }

    /**
     * Hack, add a joining type to the previous entry from the end.  This is
     * an artifact of how joins are parsed.
     */
    public void addPreviousJoin(int type, Expression on_expression) {
        join_set.addPreviousJoin(type, on_expression);
    }

    /**
     * Adds a Join to the from clause.  'type' must be a join type as defined
     * in JoiningSet, and expression represents the ON condition.
     */
    public void addJoin(int type, Expression on_expression) {
        join_set.addJoin(type, on_expression);
    }

    /**
     * Returns the JoiningSet object for the FROM clause.
     */
    public JoiningSet getJoinSet() {
        return join_set;
    }

    /**
     * Returns the type of join after table 'n' in the set of tables in the
     * from clause.  Returns, JoiningSet.INNER_JOIN, JoiningSet.FULL_OUTER_JOIN,
     * etc.
     */
    public int getJoinType(int n) {
        return getJoinSet().getJoinType(n);
    }

    /**
     * Returns the ON Expression for the type of join after table 'n' in the
     * set.
     */
    public Expression getOnExpression(int n) {
        return getJoinSet().getOnExpression(n);
    }

    /**
     * Returns a Set of FromTableDef objects that represent all the tables
     * that are in this from clause.
     */
    public Collection allTables() {
        return def_list;
    }

    // Implemented from StatementTreeObject
    public void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException {
        // Prepare expressions in the JoiningSet first
        int size = join_set.getTableCount() - 1;
        for (int i = 0; i < size; ++i) {
            Expression exp = join_set.getOnExpression(i);
            if (exp != null) {
                exp.prepare(preparer);
            }
        }
        // Prepare the StatementTree sub-queries in the from tables
        for (int i = 0; i < def_list.size(); ++i) {
            FromTableDef table_def = (FromTableDef) def_list.get(i);
            table_def.prepareExpressions(preparer);
        }

    }

    public Object clone() throws CloneNotSupportedException {
        FromClause v = (FromClause) super.clone();
        v.join_set = (JoiningSet) join_set.clone();
        ArrayList cloned_def_list = new ArrayList(def_list.size());
        v.def_list = cloned_def_list;
        v.all_table_names = (ArrayList) all_table_names.clone();

        for (int i = 0; i < def_list.size(); ++i) {
            FromTableDef table_def = (FromTableDef) def_list.get(i);
            cloned_def_list.add(table_def.clone());
        }

        return v;
    }

}
