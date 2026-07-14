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
 * A wrapper for a variable in a sub-query that references a column outside
 * of the current query.  A correlated variable differs from a regular
 * variable because its value is constant in an operation, but may vary over
 * future iterations of the operation.
 * <p>
 * This object is NOT immutable.
 *
 * @author Tobias Downer
 */

public class CorrelatedVariable implements Cloneable, java.io.Serializable {

    static final long serialVersionUID = -607848111230634419L;

    /**
     * The Variable reference itself.
     */
    private Variable variable;

    /**
     * The number of sub-query branches back that the reference for this
     * variable can be found.
     */
    private final int query_level_offset;

    /**
     * The temporary value this variable has been set to evaluate to.
     */
    private transient TObject eval_result;


    /**
     * Constructs the CorrelatedVariable.
     */
    public CorrelatedVariable(Variable variable, int level_offset) {
        this.variable = variable;
        this.query_level_offset = level_offset;
    }

    /**
     * Returns the wrapped Variable.
     */
    public Variable getVariable() {
        return variable;
    }

    /**
     * Returns the number of sub-query branches back that the reference for this
     * variable can be found.  For example, if the correlated variable references
     * the direct descendant this will return 1.
     */
    public int getQueryLevelOffset() {
        return query_level_offset;
    }

    /**
     * Sets the value this correlated variable evaluates to.
     */
    public void setEvalResult(TObject ob) {
        this.eval_result = ob;
    }

    /**
     * Given a VariableResolver this will set the value of the correlated
     * variable.
     */
    public void setFromResolver(VariableResolver resolver) {
        Variable v = getVariable();
        setEvalResult(resolver.resolve(v));
    }

    /**
     * Returns the value this correlated variable evaluates to.
     */
    public TObject getEvalResult() {
        return eval_result;
    }

    /**
     * Returns the TType this correlated variable evaluates to.
     */
    public TType returnTType() {
        return eval_result.getTType();
    }


    /**
     * Clones the object.
     */
    public Object clone() throws CloneNotSupportedException {
        CorrelatedVariable v = (CorrelatedVariable) super.clone();
        v.variable = (Variable) variable.clone();
        return v;
    }

    public String toString() {
        return "CORRELATED: " + getVariable() + " = " + getEvalResult();
    }

}
