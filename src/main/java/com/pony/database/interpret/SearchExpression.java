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

import java.util.*;

/**
 * Search expression is a form of an Expression that is split up into
 * component parts that can be easily formed into a search query.
 *
 * @author Tobias Downer
 */

public final class SearchExpression
        implements java.io.Serializable, StatementTreeObject, Cloneable {

    static final long serialVersionUID = 2888486150597671440L;

    /**
     * The originating expression.
     */
    private Expression search_expression;

    /**
     * Sets this search expression from the given expression.
     */
    public void setFromExpression(Expression expression) {
        this.search_expression = expression;
    }

    /**
     * Returns the search expression as an Expression object.
     */
    public Expression getFromExpression() {
        return search_expression;
    }

    /**
     * Concatinates a new expression to the end of this expression and uses the
     * 'AND' operator to seperate the expressions.  This is very useful for
     * adding new logical conditions to the expression at runtime.
     */
    void appendExpression(Expression expression) {
        if (search_expression == null) {
            search_expression = expression;
        } else {
            search_expression = new Expression(search_expression,
                    Operator.get("and"), expression);
        }
    }


//  /**
//   * Given a SelectStatement, this will resolve all the conditions found in
//   * this expression (reversively) to their proper full name.  If any
//   * ambiguity is found then an error is thrown.
//   */
//  void resolveColumnNames(Statement statement) {
//    if (search_expression != null) {
//      statement.resolveExpression(search_expression);
//    }
//  }
//
//  /**
//   * Evaluates the search expression.
//   */
//  TableSet evaluate(TableSet table_in, JoiningSet join_set) {
//    // Evalute the expression as a set of logical parts.
//    table_in.logicalEvaluate(search_expression, join_set);
//    return table_in;
//  }

    /**
     * Prepares the expression.
     */
    public void prepare(ExpressionPreparer preparer) throws DatabaseException {
        if (search_expression != null) {
            search_expression.prepare(preparer);
        }
    }

    /**
     * Returns all the Elements from all expressions in this condition tree.
     */
    List allElements() {
        if (search_expression != null) {
            return search_expression.allElements();
        } else {
            return new ArrayList();
        }
    }

    // Implemented from StatementTreeObject
    public void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException {
        prepare(preparer);
    }

    public Object clone() throws CloneNotSupportedException {
        SearchExpression v = (SearchExpression) super.clone();
        if (search_expression != null) {
            v.search_expression = (Expression) search_expression.clone();
        }
        return v;
    }

    public String toString() {
        if (search_expression != null) {
            return search_expression.toString();
        } else {
            return "NO SEARCH EXPRESSION";
        }
    }

}
