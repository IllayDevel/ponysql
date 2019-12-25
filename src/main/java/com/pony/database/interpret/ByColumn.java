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
 * Object used to represent a column in the 'order by' and 'group by'
 * clauses of a select statement.
 *
 * @author Tobias Downer
 */

public final class ByColumn
        implements java.io.Serializable, StatementTreeObject, Cloneable {

    static final long serialVersionUID = 8194415767416200855L;

    /**
     * The name of the column in the 'by'.
     */
    public Variable name;

    /**
     * The expression that we are ordering by.
     */
    public Expression exp;

    /**
     * If 'order by' then true if sort is ascending (default).
     */
    public boolean ascending = true;


    public void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException {
        if (exp != null) {
            exp.prepare(preparer);
        }
    }

    public Object clone() throws CloneNotSupportedException {
        ByColumn v = (ByColumn) super.clone();
        if (name != null) {
            v.name = (Variable) name.clone();
        }
        if (exp != null) {
            v.exp = (Expression) exp.clone();
        }
        return v;
    }

    public String toString() {
        return "ByColumn(" + name + ", " + exp + ", " + ascending + ")";
    }

}
