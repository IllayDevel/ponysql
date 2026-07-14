/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
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
