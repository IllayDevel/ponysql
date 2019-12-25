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

import com.pony.database.Variable;
import com.pony.database.TableName;
import com.pony.database.Expression;
import com.pony.database.DatabaseException;
import com.pony.database.StatementException;
import com.pony.database.ExpressionPreparer;
import com.pony.database.CorrelatedVariable;
import com.pony.database.DatabaseConnection;

import java.util.ArrayList;

/**
 * A set of tables and function references that make up the resources made
 * available by a table expression.  When a SelectQueriable is prepared this
 * object is created and is used to dereference names to sources.  It also
 * has the ability to chain to another TableExpressionFromSet and resolve
 * references over a complex sub-query hierarchy.
 *
 * @author Tobias Downer
 */

class TableExpressionFromSet {

    /**
     * The list of table resources in this set.
     * (FromTableInterface).
     */
    private final ArrayList table_resources;

    /**
     * The list of function expression resources.  For example, one table
     * expression may expose a function as 'SELECT (a + b) AS c, ....' in which
     * case we have a virtual assignment of c = (a + b) in this set.
     */
    private final ArrayList function_resources;

    /**
     * The list of Variable references in this set that are exposed to the
     * outside, including function aliases.  For example,
     *   SELECT a, b, c, (a + 1) d FROM ABCTable
     * Would be exposing variables 'a', 'b', 'c' and 'd'.
     */
    private final ArrayList exposed_variables;

    /**
     * Set to true if this should do case insensitive resolutions.
     */
    private boolean case_insensitive = false;

    /**
     * The parent TableExpressionFromSet if one exists.  This is used for
     * chaining a set of table sets together.  When chained the
     * 'globalResolveVariable' method can be used to resolve a reference in the
     * chain.
     */
    private TableExpressionFromSet parent;


    /**
     * Constructs the object.
     */
    public TableExpressionFromSet(DatabaseConnection connection) {
        table_resources = new ArrayList();
        function_resources = new ArrayList();
        exposed_variables = new ArrayList();
        // Is the database case insensitive?
        this.case_insensitive = connection.isInCaseInsensitiveMode();
    }

    /**
     * Sets the parent of this expression.  parent can be set to null.
     */
    public void setParent(TableExpressionFromSet parent) {
        this.parent = parent;
    }

    /**
     * Returns the parent of this set.  If it has no parent it returns null.
     */
    public TableExpressionFromSet getParent() {
        return parent;
    }

    /**
     * Toggle the case sensitivity flag.
     */
    public void setCaseInsensitive(boolean status) {
        case_insensitive = status;
    }

    boolean stringCompare(String str1, String str2) {
        if (!case_insensitive) {
            return str1.equals(str2);
        }
        return str1.equalsIgnoreCase(str2);
    }

    /**
     * Adds a table resource to the set.
     */
    public void addTable(FromTableInterface table_resource) {
        table_resources.add(table_resource);
    }

    /**
     * Adds a function resource to the set.  Note that is possible for there to
     * be references in the 'expression' that do not reference resources in this
     * set.  For example, a correlated reference.
     */
    public void addFunctionRef(String name, Expression expression) {
//    System.out.println("addFunctionRef: " + name + ", " + expression);
        function_resources.add(name);
        function_resources.add(expression);
    }

    /**
     * Adds a variable in this from set that is exposed to the outside.  This
     * list should contain all references from the SELECT ... part of the
     * query.  For example, SELECT a, b, (a + 1) d exposes variables
     * a, b and d.
     */
    public void exposeVariable(Variable v) {
//    System.out.println("exposeVariable: " + v);
//    new Error().printStackTrace();
        exposed_variables.add(v);
    }

    /**
     * Exposes all the columns from the given FromTableInterface.
     */
    public void exposeAllColumnsFromSource(FromTableInterface table) {
        Variable[] v = table.allColumns();
        for (int p = 0; p < v.length; ++p) {
            exposeVariable(v[p]);
        }
    }

    /**
     * Exposes all the columns in all the child tables.
     */
    public void exposeAllColumns() {
        for (int i = 0; i < setCount(); ++i) {
            exposeAllColumnsFromSource(getTable(i));
        }
    }

    /**
     * Exposes all the columns from the given table name.
     */
    public void exposeAllColumnsFromSource(TableName tn) {
        FromTableInterface table_interface =
                findTable(tn.getSchema(), tn.getName());
        if (table_interface == null) {
            throw new StatementException("Table name found: " + tn);
        }
        exposeAllColumnsFromSource(table_interface);
    }

    /**
     * Returns a Variable[] array for each variable that is exposed in this
     * from set.  This is a list of fully qualified variables that are
     * referencable from the final result of the table expression.
     */
    public Variable[] generateResolvedVariableList() {
        int sz = exposed_variables.size();
        Variable[] list = new Variable[sz];
        for (int i = 0; i < sz; ++i) {
            list[i] = new Variable((Variable) exposed_variables.get(i));
        }
        return list;
    }

    /**
     * Returns the first FromTableInterface object that matches the given schema,
     * table reference.  Returns null if no objects with the given schema/name
     * reference match.
     */
    FromTableInterface findTable(String schema, String name) {
        for (int p = 0; p < setCount(); ++p) {
            FromTableInterface table = getTable(p);
            if (table.matchesReference(null, schema, name)) {
                return table;
            }
        }
        return null;
    }

    /**
     * Returns the number of FromTableInterface objects in this set.
     */
    int setCount() {
        return table_resources.size();
    }

    /**
     * Returns the FromTableInterface object at the given index position in this
     * set.
     */
    FromTableInterface getTable(int i) {
        return (FromTableInterface) table_resources.get(i);
    }


    /**
     * Dereferences a fully qualified reference that is within this set.  For
     * example, SELECT ( a + b ) AS z given 'z' would return the expression
     * (a + b).
     * <p>
     * Returns null if unable to dereference assignment because it does not
     * exist.
     */
    Expression dereferenceAssignment(Variable v) {
        TableName tname = v.getTableName();
        String var_name = v.getName();
        // We are guarenteed not to match with a function if the table name part
        // of a Variable is present.
        if (tname != null) {
            return null;
        }

        // Search for the function with this name
        Expression last_found = null;
        int matches_found = 0;
        for (int i = 0; i < function_resources.size(); i += 2) {
            String fun_name = (String) function_resources.get(i);
            if (stringCompare(fun_name, var_name)) {
                if (matches_found > 0) {
                    throw new StatementException("Ambiguous reference '" + v + "'");
                }
                last_found = (Expression) function_resources.get(i + 1);
                ++matches_found;
            }
        }

        return last_found;
    }

    /**
     * Resolves the given Variable object to an assignment if it's possible to do
     * so within the context of this set.  If the variable can not be
     * unambiguously resolved to a function or aliased column, a
     * StatementException is thrown.  If the variable isn't assigned to any
     * function or aliased column, 'null' is returned.
     */
    private Variable resolveAssignmentReference(Variable v) {
        TableName tname = v.getTableName();
        String var_name = v.getName();
        // We are guarenteed not to match with a function if the table name part
        // of a Variable is present.
        if (tname != null) {
            return null;
        }

        // Search for the function with this name
        Variable last_found = null;
        int matches_found = 0;
        for (int i = 0; i < function_resources.size(); i += 2) {
            String fun_name = (String) function_resources.get(i);
            if (stringCompare(fun_name, var_name)) {
                if (matches_found > 0) {
                    throw new StatementException("Ambiguous reference '" + v + "'");
                }
                last_found = new Variable(fun_name);
                ++matches_found;
            }
        }

        return last_found;
    }


    /**
     * Resolves the given Variable against the table columns in this from set.
     * If the variable does not resolve to anything 'null' is returned.  If the
     * variable is ambiguous, a StatementException is thrown.
     * <p>
     * Note that the given variable does not have to be fully qualified but the
     * returned expressions are fully qualified.
     */
    Variable resolveTableColumnReference(Variable v) {
        TableName tname = v.getTableName();
        String sch_name = null;
        String tab_name = null;
        String col_name = v.getName();
        if (tname != null) {
            sch_name = tname.getSchema();
            tab_name = tname.getName();
        }

        // Find matches in our list of tables sources,
        Variable matched_var = null;

        for (int i = 0; i < table_resources.size(); ++i) {
            FromTableInterface table = (FromTableInterface) table_resources.get(i);
            int rcc = table.resolveColumnCount(null, sch_name, tab_name, col_name);
            if (rcc == 0) {
                // do nothing if no matches
            } else if (rcc == 1 && matched_var == null) {
                // If 1 match and matched_var = null
                matched_var =
                        table.resolveColumn(null, sch_name, tab_name, col_name);
            } else {  // if (rcc >= 1 and matched_var != null)
                System.out.println(matched_var);
                System.out.println(rcc);
                throw new StatementException("Ambiguous reference '" + v + "'");
            }
        }

        return matched_var;
    }

    /**
     * Resolves the given Variable object to a fully resolved Variable
     * within the context of this table expression.  If the variable does not
     * resolve to anything 'null' is returned.  If the variable is ambiguous, a
     * StatementException is thrown.
     * <p>
     * If the variable name references a table column, an expression with a
     * single Variable element is returned.  If the variable name references a
     * function, an expression of the function is returned.
     * <p>
     * Note that the given variable does not have to be fully qualified but the
     * returned expressions are fully qualified.
     */
    Variable resolveReference(Variable v) {
        // Try and resolve against alias names first,
        ArrayList list = new ArrayList();

//    Expression exp = dereferenceAssignment(v);
//    // If this is an alias like 'a AS b' then add 'a' to the list instead of
//    // adding 'b'.  This allows us to handle a number of ambiguous conditions.
//    if (exp != null) {
//      Variable v2 = exp.getVariable();
//      if (v2 != null) {
//        list.add(resolveTableColumnReference(v2));
//      }
//      else {
//        list.add(resolveAssignmentReference(v));
//      }
//    }

        Variable function_var = resolveAssignmentReference(v);
        if (function_var != null) {
            list.add(function_var);
        }

        Variable tc_var = resolveTableColumnReference(v);
        if (tc_var != null) {
            list.add(tc_var);
        }

//    TableName tname = v.getTableName();
//    String sch_name = null;
//    String tab_name = null;
//    String col_name = v.getName();
//    if (tname != null) {
//      sch_name = tname.getSchema();
//      tab_name = tname.getName();
//    }
//
//    // Find matches in our list of tables sources,
//    for (int i = 0; i < table_resources.size(); ++i) {
//      FromTableInterface table = (FromTableInterface) table_resources.get(i);
//      int rcc = table.resolveColumnCount(null, sch_name, tab_name, col_name);
//      if (rcc == 1) {
//        Variable matched =
//                      table.resolveColumn(null, sch_name, tab_name, col_name);
//        list.add(matched);
//      }
//      else if (rcc > 1) {
//        throw new StatementException("Ambiguous reference '" + v + "'");
//      }
//    }

        // Return the variable if we found one unambiguously.
        int list_size = list.size();
        if (list_size == 0) {
            return null;
        } else if (list_size == 1) {
            return (Variable) list.get(0);
        } else {
//      // Check if the variables are the same?
//      Variable cv = (Variable) list.get(0);
//      for (int i = 1; i < list.size(); ++i) {
//        if (!cv.equals(list.get(i))) {
            throw new StatementException("Ambiguous reference '" + v + "'");
//        }
//      }
//      // If they are all the same return the variable.
//      return v;
        }

    }

    /**
     * Resolves the given Variable reference within the chained list of
     * TableExpressionFromSet objects to a CorrelatedVariable.  If the reference
     * is not found in this set the method recurses to the parent set.  The first
     * unambiguous reference is returned.
     * <p>
     * If resolution is ambiguous within a set, a StatementException is thrown.
     * <p>
     * Returns null if the reference could not be resolved.
     */
    CorrelatedVariable globalResolveReference(int level, Variable v) {
        Variable nv = resolveReference(v);
        if (nv == null && getParent() != null) {
            // If we need to descend to the parent, increment the level.
            return getParent().globalResolveReference(level + 1, v);
        } else if (nv != null) {
            return new CorrelatedVariable(nv, level);
        }
        return null;
    }

    /**
     * Attempts to qualify the given Variable object to a value found either
     * in this from set, or a value in the parent from set.  A variable that
     * is qualified by the parent is called a correlated variable.  Any
     * correlated variables that are successfully qualified are returned as
     * CorrelatedVariable objects.
     */
    Object qualifyVariable(Variable v_in) {
        Variable v = resolveReference(v_in);
        if (v == null) {
            // If not found, try and resolve in parent set (correlated)
            if (getParent() != null) {
                CorrelatedVariable cv = getParent().globalResolveReference(1, v_in);
                if (cv == null) {
                    throw new StatementException("Reference '" +
                            v_in + "' not found.");
                }
                return cv;
            }
            if (v == null) {
                throw new StatementException("Reference '" +
                        v_in + "' not found.");
            }
        }
        return v;
    }

    /**
     * Returns an ExpressionPreparer that qualifies all variables in an
     * expression to either a qualified Variable or a CorrelatedVariable object.
     */
    ExpressionPreparer expressionQualifier() {
        return new ExpressionPreparer() {
            public boolean canPrepare(Object element) {
                return element instanceof Variable;
            }

            public Object prepare(Object element) throws DatabaseException {
                return qualifyVariable((Variable) element);
            }
        };
    }

}
