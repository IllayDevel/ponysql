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

package com.pony.database.interpret;

import com.pony.database.*;

/**
 * Represents a column selected to be in the output of a select statement.
 * This includes being either an aggregate function, a column or "*" which
 * is the entire set of columns.
 *
 * @author Tobias Downer
 */

public final class SelectColumn
        implements java.io.Serializable, StatementTreeObject, Cloneable {

    static final long serialVersionUID = 2507375247510606004L;

    /**
     * If the column represents a glob of columns (eg. 'Part.*' or '*') then
     * this is set to the glob string and 'expression' is left blank.
     */
    public String glob_name;

    /**
     * The fully resolved name that this column is given in the resulting table.
     */
    public Variable resolved_name;

    /**
     * The alias of this column string.
     */
    public String alias;

    /**
     * The expression of this column.  This is only NOT set when name == "*"
     * indicating all the columns.
     */
    public Expression expression;

    /**
     * The name of this column used internally to reference it.
     */
    public Variable internal_name;


//  /**
//   * Makes a deep clone of this object.
//   */
//  SelectColumn deepClone() {
//    SelectColumn sc = new SelectColumn();
//    sc.glob_name = glob_name;
//    sc.resolved_name = resolved_name;
//    sc.alias = alias;
//    sc.expression = new Expression(expression);
//    sc.internal_name = internal_name;
//    return sc;
//  }


    // Implemented from StatementTreeObject
    public void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException {
        if (expression != null) {
            expression.prepare(preparer);
        }
    }

    public Object clone() throws CloneNotSupportedException {
        SelectColumn v = (SelectColumn) super.clone();
        if (resolved_name != null) {
            v.resolved_name = (Variable) resolved_name.clone();
        }
        if (expression != null) {
            v.expression = (Expression) expression.clone();
        }
        if (internal_name != null) {
            v.internal_name = (Variable) internal_name.clone();
        }
        return v;
    }


    public String toString() {
        String str = "";
        if (glob_name != null) str += " GLOB_NAME = " + glob_name;
        if (resolved_name != null) str += " RESOLVED_NAME = " + resolved_name;
        if (alias != null) str += " ALIAS = " + alias;
        if (expression != null) str += " EXPRESSION = " + expression;
        if (internal_name != null) str += " INTERNAL_NAME = " + internal_name;
        return str;
    }

}
