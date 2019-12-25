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

import com.pony.database.ExpressionPreparer;
import com.pony.database.DatabaseException;

/**
 * Describes a single table declaration in the from clause of a table
 * expression (SELECT).
 *
 * @author Tobias Downer
 */

public final class FromTableDef implements java.io.Serializable, Cloneable {

    static final long serialVersionUID = -606852454508224625L;

    /**
     * If this is true, then the table def represents a sub-query table.
     * The 'getSubSelectStatement' and 'getAlias' method can be used to
     * get the table information.
     * <p>
     * eg. FROM ( SELECT id, number FROM Part ) AS part_info, ....
     */
    private final boolean subquery_table;

    /**
     * The unique key name given to this table definition.
     */
    private String unique_key;

    /**
     * The name of the table this definition references.
     */
    private final String table_name;

    /**
     * The alias of the table or null if no alias was defined.
     */
    private final String table_alias;

    /**
     * The TableSelectExpression if this is a subquery table.
     */
    private TableSelectExpression subselect_table;

    /**
     * Constructs the table def.  The constructs a table that is aliased under
     * a different name.
     */
    public FromTableDef(String table_name, String table_alias) {
        this.table_name = table_name;
        this.table_alias = table_alias;
        subselect_table = null;
        subquery_table = false;
    }

    /**
     * A simple table definition (not aliased).
     */
    public FromTableDef(String table_name) {
        this(table_name, null);
    }

    /**
     * A table that is a sub-query and given an aliased name.
     */
    public FromTableDef(TableSelectExpression select, String table_alias) {
        this.subselect_table = select;
        this.table_name = table_alias;
        this.table_alias = table_alias;
        subquery_table = true;
    }

    /**
     * A simple sub-query table definition (not aliased).
     */
    public FromTableDef(TableSelectExpression select) {
        this.subselect_table = select;
        this.table_name = null;
        this.table_alias = null;
        subquery_table = true;
    }


    /**
     * Sets the unique key.
     */
    public void setUniqueKey(String unique_key) {
        this.unique_key = unique_key;
    }

    /**
     * Returns the name of the table.
     */
    public String getName() {
        return table_name;
    }

    /**
     * Returns the alias for this table (or null if no alias given).
     */
    public String getAlias() {
        return table_alias;
    }

    /**
     * Returns the unique key.
     */
    public String getUniqueKey() {
        return unique_key;
    }

    /**
     * Returns true if this item in the FROM clause is a subquery table.
     */
    public boolean isSubQueryTable() {
        return subquery_table;
    }

    /**
     * Returns the TableSelectExpression if this is a subquery table.
     */
    public TableSelectExpression getTableSelectExpression() {
        return subselect_table;
    }

    /**
     * Prepares the expressions in this table def.
     */
    public void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException {
        if (subselect_table != null) {
            subselect_table.prepareExpressions(preparer);
        }
    }

    /**
     * Clones the object (deep clone of immutable members).
     */
    public Object clone() throws CloneNotSupportedException {
        FromTableDef v = (FromTableDef) super.clone();
        if (subselect_table != null) {
            v.subselect_table = (TableSelectExpression) subselect_table.clone();
        }
        return v;
    }

}
