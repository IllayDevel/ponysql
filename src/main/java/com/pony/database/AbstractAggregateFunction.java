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
 * Provides convenience methods for handling aggregate functions (functions
 * that are evaluated over a grouping set).  Note that this class handles the
 * most common form of aggregate functions.  These are aggregates with no more
 * or no less than one parameter, and that return NULL if the group set has a
 * length of 0.  If an aggregate function doesn't fit this design, then the
 * developer must roll their own AbstractFunction to handle it.
 * <p>
 * This object handles full expressions being passed as parameters to the
 * aggregate function.  The expression is evaluated for each set in the
 * group.  Therefore the aggregate function, avg(length(description)) will
 * find the average length of the description column.  sum(price * quantity)
 * will find the sum of the price * quantity of each set in the group.
 *
 * @author Tobias Downer
 */

public abstract class AbstractAggregateFunction extends AbstractFunction {

    /**
     * Constructs an aggregate function.
     */
    public AbstractAggregateFunction(String name, Expression[] params) {
        super(name, params);
        setAggregate(true);

        // Aggregates must have only one argument
        if (parameterCount() != 1) {
            throw new RuntimeException("'" + name +
                    "' function must have one argument.");
        }

    }

    // ---------- Abstract ----------

    /**
     * Evaluates the aggregate function for the given values and returns the
     * result.  If this aggregate was 'sum' then this method would sum the two
     * values.  If this aggregate was 'avg' then this method would also sum the
     * two values and the 'postEvalAggregate' would divide by the number
     * processed.
     * <p>
     * NOTE: This first time this method is called on a set, 'val1' is 'null' and
     * 'val2' contains the first value in the set.
     */
    public abstract TObject evalAggregate(GroupResolver group,
                                          QueryContext context,
                                          TObject val1, TObject val2);

    /**
     * Called just before the value is returned to the parent.  This does any
     * final processing on the result before it is returned.  If this aggregate
     * was 'avg' then we'd divide by the size of the group.
     */
    public TObject postEvalAggregate(GroupResolver group,
                                     QueryContext context,
                                     TObject result) {
        // By default, do nothing....
        return result;
    }


    // ---------- Implemented from AbstractFunction ----------

    public final TObject evaluate(GroupResolver group,
                                  VariableResolver resolver,
                                  QueryContext context) {
        if (group == null) {
            throw new RuntimeException("'" + getName() +
                    "' can only be used as an aggregate function.");
        }

        TObject result = null;
        // All aggregates functions return 'null' if group size is 0
        int size = group.size();
        if (size == 0) {
            // Return a NULL of the return type
            return new TObject(returnTType(resolver, context), null);
        }

        TObject val;
        Variable v = getParameter(0).getVariable();
        // If the aggregate parameter is a simple variable, then use optimal
        // routine,
        if (v != null) {
            for (int i = 0; i < size; ++i) {
                val = group.resolve(v, i);
                result = evalAggregate(group, context, result, val);
            }
        } else {
            // Otherwise we must resolve the expression for each entry in group,
            // This allows for expressions such as 'sum(quantity * price)' to
            // work for a group.
            Expression exp = getParameter(0);
            for (int i = 0; i < size; ++i) {
                val = exp.evaluate(null, group.getVariableResolver(i), context);
                result = evalAggregate(group, context, result, val);
            }
        }

        // Post method.
        result = postEvalAggregate(group, context, result);

        return result;
    }

}
