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
