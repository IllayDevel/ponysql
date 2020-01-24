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

import java.util.List;

/**
 * Represents a function that is part of an expression to be evaluated.  A
 * function evaluates to a resultant Object.  If the parameters of a function
 * are not constant values, then the evaluation will require a lookup via a
 * VariableResolver or GroupResolver.  The GroupResolver helps evaluate an
 * aggregate function.
 *
 * @author Tobias Downer
 */

public interface Function {

    /**
     * Returns the name of the function.  The name is a unique identifier that
     * can be used to recreate this function.  This identifier can be used to
     * easily serialize the function when grouped with its parameters.
     */
    String getName();

    /**
     * Returns the list of Variable objects that this function uses as its
     * parameters.  If this returns an empty list, then the function must
     * only have constant parameters.  This information can be used to optimize
     * evaluation because if all the parameters of a function are constant then
     * we only need to evaluate the function once.
     */
    List allVariables();

    /**
     * Returns the list of all element objects that this function uses as its
     * parameters.  If this returns an empty list, then the function has no
     * input elements at all.  ( something like: upper(user()) )
     */
    List allElements();

    /**
     * Returns true if this function is an aggregate function.  An aggregate
     * function requires that the GroupResolver is not null when the evaluate
     * method is called.
     */
    boolean isAggregate(QueryContext context);

    /**
     * Prepares the exressions that are the parameters of this function.  This
     * is intended to be used if we need to resolve aspects such as Variable
     * references.  For example, a variable reference to 'number' may become
     * 'APP.Table.NUMBER'.
     */
    void prepareParameters(ExpressionPreparer preparer)
            throws DatabaseException;

    /**
     * Evaluates the function and returns a TObject that represents the result
     * of the function.  The VariableResolver object should be used to look
     * up variables in the parameter of the function.  The 'FunctionTable'
     * object should only be used when the function is a grouping function.  For
     * example, 'avg(value_of)'.
     */
    TObject evaluate(GroupResolver group, VariableResolver resolver,
                     QueryContext context);

    /**
     * The type of object this function returns.  eg. TStringType,
     * TBooleanType, etc.  The VariableResolver points to a dummy row that can
     * be used to dynamically determine the return type.  For example, an
     * implementation of SQL 'GREATEST' would return the same type as the
     * list elements.
     */
    TType returnTType(VariableResolver resolver, QueryContext context);

}
