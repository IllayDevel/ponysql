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

package com.pony.database;

/**
 * An assignment from a variable to an expression.  For example;<p>
 * <pre>
 *   value_of = value_of * 1.10
 *   name = concat("CS-", name)
 *   description = concat("LEGACY: ", upper(number));
 * </pre>
 *
 * @author Tobias Downer
 */

public final class Assignment
        implements StatementTreeObject, java.io.Serializable, Cloneable {

    static final long serialVersionUID = 498589698743066869L;

    /**
     * The Variable that is the lhs of the assignment.
     */
    private Variable variable;

    /**
     * Set expression that is the rhs of the assignment.
     */
    private Expression expression;

    /**
     * Constructs the assignment.
     */
    public Assignment(Variable variable, Expression expression) {
        this.variable = variable;
        this.expression = expression;
    }

    /**
     * Returns the variable for this assignment.
     */
    public Variable getVariable() {
        return variable;
    }

    /**
     * Returns the Expression for this assignment.
     */
    public Expression getExpression() {
        return expression;
    }

    // ---------- Implemented from StatementTreeObject ----------
    public void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException {
        if (expression != null) {
            expression.prepare(preparer);
        }
    }

    public Object clone() throws CloneNotSupportedException {
        Assignment v = (Assignment) super.clone();
        v.variable = (Variable) variable.clone();
        v.expression = (Expression) expression.clone();
        return v;
    }

}
