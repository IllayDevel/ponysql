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

package com.pony.database;

import java.util.List;
import java.util.ArrayList;
import java.io.StringReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.pony.database.sql.SQL;
import com.pony.util.BigNumber;

/**
 * An expression that can be evaluated in a statement.  This is used as a more
 * complete and flexible version of 'Condition' as well as representing column
 * and aggregate functions.
 * <p>
 * This class can represent constant expressions (expressions that have no
 * variable input), as well as variable expressions.  Optimizations may be
 * possible when evaluating constant expressions.
 * <p>
 * Some examples of constant expressions;<p><pre>
 *   ( 9 + 3 ) * 90
 *   ( ? * 9 ) / 1
 *   lower("CaPS MUMma")
 *   40 & 0x0FF != 39
 * </pre>
 * Some examples of variable expressions;<p><pre>
 *   upper(Part.description)
 *   Part.id >= 50
 *   VendorMakesPart.part_id == Part.id
 *   Part.value_of <= Part.cost_of / 4
 * </pre>
 * <p>
 * <strong>NOTE:</strong> the expression is stored in postfix orientation.  eg.
 *   "8 + 9 * 3" becomes "8,9,3,*,+"
 * <p>
 * <strong>NOTE:</strong> This class is <b>NOT</b> thread safe.  Do not use an
 *   expression instance between threads.
 *
 * @author Tobias Downer
 */

public final class Expression implements java.io.Serializable, Cloneable {

    /**
     * Serialization UID.
     */
    static final long serialVersionUID = 6981261114471924028L;

    /**
     * The list of elements followed by operators in our expression.  The
     * expression elements may be of any type represented by the database
     * (see 'addElement' method for the accepted objects).  The expression
     * operators may be '+', '-', '*', '*', '/', '=', '>=', '<>', etc (as an
     * Operator object (see the Operator class for details)).
     * <p>
     * This list is stored in postfix order.
     */
    private ArrayList elements = new ArrayList();

    /**
     * The evaluation stack for when the expression is evaluated.
     */
    private transient ArrayList eval_stack;

    /**
     * The expression as a plain human readable string.  This is in a form that
     * can be readily parsed to an Expression object.
     */
    private StringBuffer text;


    /**
     * Constructs a new Expression.
     */
    public Expression() {
        text = new StringBuffer();
    }

    /**
     * Constructs a new Expression with a single object element.
     */
    public Expression(Object ob) {
        this();
        addElement(ob);
    }

    /**
     * Constructs a copy of the given Expression.
     */
    public Expression(Expression exp) {
        concat(exp);
        text = new StringBuffer(new String(exp.text));
    }

    /**
     * Constructs a new Expression from the concatination of expression1 and
     * expression2 and the operator for them.
     */
    public Expression(Expression exp1, Operator op, Expression exp2) {
        // Remember, this is in postfix notation.
        elements.addAll(exp1.elements);
        elements.addAll(exp2.elements);
        elements.add(op);
    }

    /**
     * Returns the StringBuffer that we can use to append plain text
     * representation as we are parsing the expression.
     */
    public StringBuffer text() {
        return text;
    }

    /**
     * Copies the text from the given expression.
     */
    public void copyTextFrom(Expression e) {
        this.text = new StringBuffer(new String(e.text()));
    }

    /**
     * Static method that parses the given string which contains an expression
     * into an Expression object.  For example, this will parse strings such
     * as '(a + 9) * 2 = b' or 'upper(concat('12', '56', id))'.
     * <p>
     * Care should be taken to not use this method inside an inner loop because
     * it creates a lot of objects.
     */
    public static Expression parse(String expression) {
        synchronized (expression_parser) {
            try {
                expression_parser.ReInit(new StringReader(expression));
                expression_parser.reset();
                Expression exp = expression_parser.parseExpression();

                exp.text().setLength(0);
                exp.text().append(expression);
                return exp;
            } catch (com.pony.database.sql.ParseException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    /**
     * A static expression parser.  To use this we must first synchronize over
     * the object.
     */
    private final static SQL expression_parser = new SQL(new StringReader(""));

    /**
     * Generates a simple expression from two objects and an operator.
     */
    public static Expression simple(Object ob1, Operator op, Object ob2) {
        Expression exp = new Expression(ob1);
        exp.addElement(ob2);
        exp.addElement(op);
        return exp;
    }


    /**
     * Adds a new element into the expression.  String, BigNumber, Boolean and
     * Variable are the types of elements allowed.
     * <p>
     * Must be added in postfix order.
     */
    public void addElement(Object ob) {
        if (ob == null) {
            elements.add(TObject.nullVal());
        } else if (ob instanceof TObject ||
                ob instanceof ParameterSubstitution ||
                ob instanceof CorrelatedVariable ||
                ob instanceof Variable ||
                ob instanceof FunctionDef ||
                ob instanceof Operator ||
                ob instanceof StatementTreeObject
        ) {
            elements.add(ob);
        } else {
            throw new Error("Unknown element type added to expression: " +
                    ob.getClass());
        }
    }

    /**
     * Merges an expression with this expression.  For example, given the
     * expression 'ab', if the expression 'abc+-' was added the expression would
     * become 'ababc+-'.
     * <p>
     * This method is useful when copying parts of other expressions when forming
     * an expression.
     * <p>
     * This always returns this expression.  This does not change 'text()'.
     */
    public Expression concat(Expression expr) {
        elements.addAll(expr.elements);
        return this;
    }

    /**
     * Adds a new operator into the expression.  Operators are represented as
     * an Operator (eg. ">", "+", "<<", "!=" )
     * <p>
     * Must be added in postfix order.
     */
    public void addOperator(Operator op) {
        elements.add(op);
    }

    /**
     * Returns the number of elements and operators that are in this postfix
     * list.
     */
    public int size() {
        return elements.size();
    }

    /**
     * Returns the element at the given position in the postfix list.  Note, this
     * can return Operator's.
     */
    public Object elementAt(int n) {
        return elements.get(n);
    }

    /**
     * Returns the element at the end of the postfix list (the last element).
     */
    public Object last() {
        return elements.get(size() - 1);
    }


    /**
     * Sets the element at the given position in the postfix list.  This should
     * be called after the expression has been setup to alter variable alias
     * names, etc.
     */
    public void setElementAt(int n, Object ob) {
        elements.set(n, ob);
    }

    /**
     * Pushes an element onto the evaluation stack.
     */
    private void push(Object ob) {
        eval_stack.add(ob);
    }

    /**
     * Pops an element from the evaluation stack.
     */
    private Object pop() {
        return eval_stack.remove(eval_stack.size() - 1);
    }


    /**
     * Returns a complete List of Variable objects in this expression not
     * including correlated variables.
     */
    public List allVariables() {
        ArrayList vars = new ArrayList();
        for (int i = 0; i < elements.size(); ++i) {
            Object ob = elements.get(i);
            if (ob instanceof Variable) {
                vars.add(ob);
            } else if (ob instanceof FunctionDef) {
                Expression[] params = ((FunctionDef) ob).getParameters();
                for (int n = 0; n < params.length; ++n) {
                    vars.addAll(params[n].allVariables());
                }
            } else if (ob instanceof TObject) {
                TObject tob = (TObject) ob;
                if (tob.getTType() instanceof TArrayType) {
                    Expression[] exp_list = (Expression[]) tob.getObject();
                    for (int n = 0; n < exp_list.length; ++n) {
                        vars.addAll(exp_list[n].allVariables());
                    }
                }
            }
        }
        return vars;
    }

    /**
     * Returns a complete list of all element objects that are in this expression
     * and in the parameters of the functions of this expression.
     */
    public List allElements() {
        ArrayList elems = new ArrayList();
        for (int i = 0; i < elements.size(); ++i) {
            Object ob = elements.get(i);
            if (ob instanceof Operator) {
            } else if (ob instanceof FunctionDef) {
                Expression[] params = ((FunctionDef) ob).getParameters();
                for (int n = 0; n < params.length; ++n) {
                    elems.addAll(params[n].allElements());
                }
            } else if (ob instanceof TObject) {
                TObject tob = (TObject) ob;
                if (tob.getTType() instanceof TArrayType) {
                    Expression[] exp_list = (Expression[]) tob.getObject();
                    for (int n = 0; n < exp_list.length; ++n) {
                        elems.addAll(exp_list[n].allElements());
                    }
                } else {
                    elems.add(ob);
                }
            } else {
                elems.add(ob);
            }
        }
        return elems;
    }

    /**
     * A general prepare that cascades through the expression and its parents and
     * substitutes an elements that the preparer wants to substitute.
     * <p>
     * NOTE: This will not cascade through to the parameters of Function objects
     *   however it will cascade through FunctionDef parameters.  For this
     *   reason you MUST call 'prepareFunctions' after this method.
     */
    public void prepare(ExpressionPreparer preparer) throws DatabaseException {
        for (int n = 0; n < elements.size(); ++n) {
            Object ob = elements.get(n);

            // If the preparer will prepare this type of object then set the
            // entry with the prepared object.
            if (preparer.canPrepare(ob)) {
                elements.set(n, preparer.prepare(ob));
            }

            Expression[] exp_list = null;
            if (ob instanceof FunctionDef) {
                FunctionDef func = (FunctionDef) ob;
                exp_list = func.getParameters();
            } else if (ob instanceof TObject) {
                TObject tob = (TObject) ob;
                if (tob.getTType() instanceof TArrayType) {
                    exp_list = (Expression[]) tob.getObject();
                }
            } else if (ob instanceof StatementTreeObject) {
                StatementTreeObject stree = (StatementTreeObject) ob;
                stree.prepareExpressions(preparer);
            }

            if (exp_list != null) {
                for (int p = 0; p < exp_list.length; ++p) {
                    exp_list[p].prepare(preparer);
                }
            }

        }
    }


    /**
     * Returns true if the expression doesn't include any variables or non
     * constant functions (is constant).  Note that a correlated variable is
     * considered a constant.
     */
    public boolean isConstant() {
        for (int n = 0; n < elements.size(); ++n) {
            Object ob = elements.get(n);
            if (ob instanceof TObject) {
                TObject tob = (TObject) ob;
                TType ttype = tob.getTType();
                // If this is a query plan, return false
                if (ttype instanceof TQueryPlanType) {
                    return false;
                }
                // If this is an array, check the array for constants
                else if (ttype instanceof TArrayType) {
                    Expression[] exp_list = (Expression[]) tob.getObject();
                    for (int p = 0; p < exp_list.length; ++p) {
                        if (!exp_list[p].isConstant()) {
                            return false;
                        }
                    }
                }
            } else if (ob instanceof Variable) {
                return false;
            } else if (ob instanceof FunctionDef) {
                Expression[] params = ((FunctionDef) ob).getParameters();
                for (int p = 0; p < params.length; ++p) {
                    if (!params[p].isConstant()) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Returns true if the expression has a subquery (eg 'in ( select ... )')
     * somewhere in it (this cascades through function parameters also).
     */
    public boolean hasSubQuery() {
        List list = allElements();
        int len = list.size();
        for (int n = 0; n < len; ++n) {
            Object ob = list.get(n);
            if (ob instanceof TObject) {
                TObject tob = (TObject) ob;
                if (tob.getTType() instanceof TQueryPlanType) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns true if the expression contains a NOT operator somewhere in it.
     */
    public boolean containsNotOperator() {
        for (int n = 0; n < elements.size(); ++n) {
            Object ob = elements.get(n);
            if (ob instanceof Operator) {
                if (((Operator) ob).isNot()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Discovers all the correlated variables in this expression.  If this
     * expression contains a sub-query plan, we ask the plan to find the list of
     * correlated variables.  The discovery process increments the 'level'
     * variable for each sub-plan.
     */
    public ArrayList discoverCorrelatedVariables(int level, ArrayList list) {
        List elems = allElements();
        int sz = elems.size();
        // For each element
        for (int i = 0; i < sz; ++i) {
            Object ob = elems.get(i);
            if (ob instanceof CorrelatedVariable) {
                CorrelatedVariable v = (CorrelatedVariable) ob;
                if (v.getQueryLevelOffset() == level) {
                    list.add(v);
                }
            } else if (ob instanceof TObject) {
                TObject tob = (TObject) ob;
                if (tob.getTType() instanceof TQueryPlanType) {
                    QueryPlanNode node = (QueryPlanNode) tob.getObject();
                    list = node.discoverCorrelatedVariables(level + 1, list);
                }
            }
        }
        return list;
    }

    /**
     * Discovers all the tables in the sub-queries of this expression.  This is
     * used for determining all the tables that a query plan touches.
     */
    public ArrayList discoverTableNames(ArrayList list) {
        List elems = allElements();
        int sz = elems.size();
        // For each element
        for (int i = 0; i < sz; ++i) {
            Object ob = elems.get(i);
            if (ob instanceof TObject) {
                TObject tob = (TObject) ob;
                if (tob.getTType() instanceof TQueryPlanType) {
                    QueryPlanNode node = (QueryPlanNode) tob.getObject();
                    list = node.discoverTableNames(list);
                }
            }
        }
        return list;
    }

    /**
     * Returns the QueryPlanNode object in this expression if it evaluates to a
     * single QueryPlanNode, otherwise returns null.
     */
    public QueryPlanNode getQueryPlanNode() {
        Object ob = elementAt(0);
        if (size() == 1 && ob instanceof TObject) {
            TObject tob = (TObject) ob;
            if (tob.getTType() instanceof TQueryPlanType) {
                return (QueryPlanNode) tob.getObject();
            }
        }
        return null;
    }

    /**
     * Returns the Variable if this expression evaluates to a single variable,
     * otherwise returns null.  A correlated variable will not be returned.
     */
    public Variable getVariable() {
        Object ob = elementAt(0);
        if (size() == 1 && ob instanceof Variable) {
            return (Variable) ob;
        }
        return null;
    }

    /**
     * Returns an array of two Expression objects that represent the left hand
     * and right and side of the last operator in the post fix notation.
     * For example, (a + b) - (c + d) will return { (a + b), (c + d) }.  Or
     * more a more useful example is;<p><pre>
     *   id + 3 > part_id - 2 will return ( id + 3, part_id - 2 }
     * </pre>
     */
    public Expression[] split() {
        if (size() <= 1) {
            throw new Error("Can only split expressions with more than 1 element.");
        }

        int midpoint = -1;
        int stack_size = 0;
        for (int n = 0; n < size() - 1; ++n) {
            Object ob = elementAt(n);
            if (ob instanceof Operator) {
                --stack_size;
            } else {
                ++stack_size;
            }

            if (stack_size == 1) {
                midpoint = n;
            }
        }

        if (midpoint == -1) {
            throw new Error("postfix format error: Midpoint not found.");
        }

        Expression lhs = new Expression();
        for (int n = 0; n <= midpoint; ++n) {
            lhs.addElement(elementAt(n));
        }

        Expression rhs = new Expression();
        for (int n = midpoint + 1; n < size() - 1; ++n) {
            rhs.addElement(elementAt(n));
        }

        return new Expression[]{lhs, rhs};
    }

    /**
     * Returns the end Expression of this expression.  For example, an expression
     * of 'ab' has an end expression of 'b'.  The expression 'abc+=' has an end
     * expression of 'abc+='.
     * <p>
     * This is a useful method to call in the middle of an Expression object
     * being formed.  It allows for the last complete expression to be
     * returned.
     * <p>
     * If this is called when an expression is completely formed it will always
     * return the complete expression.
     */
    public Expression getEndExpression() {

        int stack_size = 1;
        int end = size() - 1;
        for (int n = end; n > 0; --n) {
            Object ob = elementAt(n);
            if (ob instanceof Operator) {
                ++stack_size;
            } else {
                --stack_size;
            }

            if (stack_size == 0) {
                // Now, n .. end represents the new expression.
                Expression new_exp = new Expression();
                for (int i = n; i <= end; ++i) {
                    new_exp.addElement(elementAt(i));
                }
                return new_exp;
            }
        }

        return new Expression(this);
    }

    /**
     * Breaks this expression into a list of sub-expressions that are split
     * by the given operator.  For example, given the expression;
     * <p><pre>
     *   (a = b AND b = c AND (a = 2 OR c = 1))
     * </pre><p>
     * Calling this method with logical_op = "and" will return a list of the
     * three expressions.
     * <p>
     * This is a common function used to split up an expressions into logical
     * components for processing.
     */
    public ArrayList breakByOperator(ArrayList list, final String logical_op) {
        // The last operator must be 'and'
        Object ob = last();
        if (ob instanceof Operator) {
            Operator op = (Operator) ob;
            if (op.is(logical_op)) {
                // Last operator is 'and' so split and recurse.
                Expression[] exps = split();
                list = exps[0].breakByOperator(list, logical_op);
                list = exps[1].breakByOperator(list, logical_op);
                return list;
            }
        }
        // If no last expression that matches then add this expression to the
        // list.
        list.add(this);
        return list;
    }

    /**
     * Evaluates this expression and returns an Object that represents the
     * result of the evaluation.  The passed VariableResolver object is used
     * to resolve the variable name to a value.  The GroupResolver object is
     * used if there are any aggregate functions in the evaluation - this can be
     * null if evaluating an expression without grouping aggregates.  The query
     * context object contains contextual information about the environment of
     * the query.
     * <p>
     * NOTE: This method is gonna be called a lot, so we need it to be optimal.
     * <p>
     * NOTE: This method is <b>not</b> thread safe!  The reason it's not safe
     *   is because of the evaluation stack.
     */
    public TObject evaluate(GroupResolver group, VariableResolver resolver,
                            QueryContext context) {
        // Optimization - trivial case of 'a' or 'ab*' postfix are tested for
        //   here.
        int element_count = elements.size();
        if (element_count == 1) {
            return (TObject) elementToObject(0, group, resolver, context);
        } else if (element_count == 3) {
            TObject o1 = (TObject) elementToObject(0, group, resolver, context);
            TObject o2 = (TObject) elementToObject(1, group, resolver, context);
            Operator op = (Operator) elements.get(2);
            return op.eval(o1, o2, group, resolver, context);
        }

        if (eval_stack == null) {
            eval_stack = new ArrayList();
        }

        for (int n = 0; n < element_count; ++n) {
            Object val = elementToObject(n, group, resolver, context);
            if (val instanceof Operator) {
                // Pop the last two values off the stack, evaluate them and push
                // the new value back on.
                Operator op = (Operator) val;

                TObject v2 = (TObject) pop();
                TObject v1 = (TObject) pop();

                push(op.eval(v1, v2, group, resolver, context));
            } else {
                push(val);
            }
        }
        // We should end with a single value on the stack.
        return (TObject) pop();
    }

    /**
     * Evaluation without a grouping table.
     */
    public TObject evaluate(VariableResolver resolver, QueryContext context) {
        return evaluate(null, resolver, context);
    }

    /**
     * Returns the element at the given position in the expression list.  If
     * the element is a variable then it is resolved on the VariableResolver.
     * If the element is a function then it is evaluated and the result is
     * returned.
     */
    private Object elementToObject(int n, GroupResolver group,
                                   VariableResolver resolver, QueryContext context) {
        Object ob = elements.get(n);
        if (ob instanceof TObject ||
                ob instanceof Operator) {
            return ob;
        } else if (ob instanceof Variable) {
            return resolver.resolve((Variable) ob);
        } else if (ob instanceof CorrelatedVariable) {
            return ((CorrelatedVariable) ob).getEvalResult();
        } else if (ob instanceof FunctionDef) {
            Function fun = ((FunctionDef) ob).getFunction(context);
            return fun.evaluate(group, resolver, context);
        } else {
            if (ob == null) {
                throw new NullPointerException("Null element in expression");
            }
            throw new Error("Unknown element type: " + ob.getClass());
        }
    }

    /**
     * Cascades through the expression and if any aggregate functions are found
     * returns true, otherwise returns false.
     */
    public boolean hasAggregateFunction(QueryContext context) {
        for (int n = 0; n < elements.size(); ++n) {
            Object ob = elements.get(n);
            if (ob instanceof FunctionDef) {
                if (((FunctionDef) ob).isAggregate(context)) {
                    return true;
                }
            } else if (ob instanceof TObject) {
                TObject tob = (TObject) ob;
                if (tob.getTType() instanceof TArrayType) {
                    Expression[] list = (Expression[]) tob.getObject();
                    for (int i = 0; i < list.length; ++i) {
                        if (list[i].hasAggregateFunction(context)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * Determines the type of object this expression evaluates to.  We determine
     * this by looking at the last element of the expression.  If the last
     * element is a TType object, it returns the type.  If the last element is a
     * Function, Operator or Variable then it returns the type that these
     * objects have set as their result type.
     */
    public TType returnTType(VariableResolver resolver, QueryContext context) {
        Object ob = elements.get(elements.size() - 1);
        if (ob instanceof FunctionDef) {
            Function fun = ((FunctionDef) ob).getFunction(context);
            return fun.returnTType(resolver, context);
        } else if (ob instanceof TObject) {
            return ((TObject) ob).getTType();
        } else if (ob instanceof Operator) {
            Operator op = (Operator) ob;
            return op.returnTType();
        } else if (ob instanceof Variable) {
            Variable variable = (Variable) ob;
            return resolver.returnTType(variable);
        } else if (ob instanceof CorrelatedVariable) {
            CorrelatedVariable variable = (CorrelatedVariable) ob;
            return variable.returnTType();
        } else {
            throw new Error("Unable to determine type for expression.");
        }
    }

    /**
     * Performs a deep clone of this object, calling 'clone' on any elements
     * that are mutable or shallow copying immutable members.
     */
    public Object clone() throws CloneNotSupportedException {
        // Shallow clone
        Expression v = (Expression) super.clone();
        v.eval_stack = null;
//    v.text = new StringBuffer(new String(text));
        int size = elements.size();
        ArrayList cloned_elements = new ArrayList(size);
        v.elements = cloned_elements;

        // Clone items in the elements list
        for (int i = 0; i < size; ++i) {
            Object element = elements.get(i);

            if (element instanceof TObject) {
                // TObject is immutable except for TArrayType and TQueryPlanType
                TObject tob = (TObject) element;
                TType ttype = tob.getTType();
                // For a query plan
                if (ttype instanceof TQueryPlanType) {
                    QueryPlanNode node = (QueryPlanNode) tob.getObject();
                    node = (QueryPlanNode) node.clone();
                    element = new TObject(ttype, node);
                }
                // For an array
                else if (ttype instanceof TArrayType) {
                    Expression[] arr = (Expression[]) tob.getObject();
                    arr = (Expression[]) arr.clone();
                    for (int n = 0; n < arr.length; ++n) {
                        arr[n] = (Expression) arr[n].clone();
                    }
                    element = new TObject(ttype, arr);
                }
            } else if (element instanceof Operator ||
                    element instanceof ParameterSubstitution) {
                // immutable so we do not need to clone these
            } else if (element instanceof CorrelatedVariable) {
                element = ((CorrelatedVariable) element).clone();
            } else if (element instanceof Variable) {
                element = ((Variable) element).clone();
            } else if (element instanceof FunctionDef) {
                element = ((FunctionDef) element).clone();
            } else if (element instanceof StatementTreeObject) {
                element = ((StatementTreeObject) element).clone();
            } else {
                throw new CloneNotSupportedException(element.getClass().toString());
            }
            cloned_elements.add(element);
        }

        return v;
    }

    /**
     * Returns a string representation of this object for diagnostic
     * purposes.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[ Expression ");
        if (text() != null) {
            buf.append("[");
            buf.append(text().toString());
            buf.append("]");
        }
        buf.append(": ");
        for (int n = 0; n < elements.size(); ++n) {
            buf.append(elements.get(n));
            if (n < elements.size() - 1) {
                buf.append(",");
            }
        }
        buf.append(" ]");
        return new String(buf);
    }

    // ---------- Serialization methods ----------

    /**
     * Writes the state of this object to the object stream.  This method is
     * implemented because GCJ doesn't like it if you implement readObject
     * without writeObject.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    /**
     * Reads the state of this object from the object stream.
     */
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // This converts old types to the new TObject type system introduced
        // in version 0.94.
        int sz = elements.size();
        for (int i = 0; i < sz; ++i) {
            Object ob = elements.get(i);
            TObject conv_object = null;
            if (ob == null || ob instanceof com.pony.database.global.NullObject) {
                conv_object = TObject.nullVal();
            } else if (ob instanceof String) {
                conv_object = TObject.stringVal((String) ob);
            } else if (ob instanceof java.math.BigDecimal) {
                conv_object = TObject.bigNumberVal(
                        BigNumber.fromBigDecimal((java.math.BigDecimal) ob));
            } else if (ob instanceof java.util.Date) {
                conv_object = TObject.dateVal((java.util.Date) ob);
            } else if (ob instanceof Boolean) {
                conv_object = TObject.booleanVal(((Boolean) ob).booleanValue());
            }
            if (conv_object != null) {
                elements.set(i, conv_object);
            }
        }
    }

}
