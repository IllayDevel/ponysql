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

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.math.BigDecimal;

/**
 * A serializable container class for a parsed query language statement.  The
 * structure of the tree is entirely dependant on the grammar that was used
 * to create the tree.  This object is a convenient form that can be cached and
 * serialized to be stored.
 * <p>
 * Think of this as the model of a query after the grammar has been parsed
 * and before it is evaluated.
 *
 * @author Tobias Downer
 */

public final class StatementTree implements java.io.Serializable, Cloneable {

    static final long serialVersionUID = -5907058730080713004L;

    /**
     * The class of statement this is.  This is set to one of the query objects
     * from the com.pony.database.interpret package.  For example, if this is
     * a select statement then it points to
     * 'com.pony.database.interpret.Select'.
     */
    private String statement_class;

    /**
     * A map that maps from the name of the tree element to the object
     * that contains information about.  For example, if this is an SQL
     * SELECT statement then entries in this map may be;
     * <pre>
     *   "columns" -> sql.SelectColumn[]
     *   "distinct" -> new Boolean(true)
     * </pre>
     */
    private HashMap map;

    /**
     * Constructs the StatementTree.
     *
     * @param statement_class the name of the class that interpretes this
     *   statement (eg. com.pony.database.interpret.Select).
     */
    public StatementTree(String statement_class) {
        if (!statement_class.startsWith("com.pony.database.interpret.")) {
            throw new Error("statement_class must be in the " +
                    "com.pony.database.interpret package.");
        }
        this.statement_class = statement_class;
        map = new HashMap();
    }

    /**
     * Puts a new entry into the statement tree map.
     */
    public void putObject(String entry_name, Object ob) {
        if (entry_name == null) {
            throw new NullPointerException("entry_name is null.");
        }
        // Check on is derived from a known class
        if (ob == null ||
                ob instanceof Boolean ||
                ob instanceof String ||
                ob instanceof BigDecimal ||
                ob instanceof Variable ||
                ob instanceof Integer ||
                ob instanceof TObject ||
                ob instanceof TType ||
                ob instanceof Expression ||
                ob instanceof Expression[] ||
                ob instanceof List ||
                ob instanceof StatementTree ||
                ob instanceof StatementTreeObject) {

            Object v = map.put(entry_name, ob);
            if (v != null) {
                throw new Error("Entry '" + entry_name +
                        "' is already present in this tree.");
            }

        } else {
            throw new Error("ob of entry '" + entry_name +
                    "' is not derived from a recognised class");
        }

    }

    /**
     * Puts a boolean into the statement tree map.
     */
    public void putBoolean(String entry_name, boolean b) {
        putObject(entry_name, b ? Boolean.TRUE : Boolean.FALSE);
    }

    /**
     * Puts an integer into the statement tree map.
     */
    public void putInt(String entry_name, int v) {
        putObject(entry_name, v);
    }


    /**
     * Gets an object entry from the statement tree.
     */
    public Object getObject(String entry_name) {
        return map.get(entry_name);
    }

    /**
     * Gets a boolean entry from the statement tree.
     */
    public boolean getBoolean(String entry_name) {
        Object ob = map.get(entry_name);
        return (Boolean) ob;
    }

    /**
     * Gets an integer entry from the statement tree.
     */
    public int getInt(String entry_name) {
        Object ob = map.get(entry_name);
        return (Integer) ob;
    }


    /**
     * Gets the interpreter class that services this tree.
     */
    public String getClassName() {
        return statement_class;
    }

    /**
     * For each expression in this StatementTree this method will call the
     * 'prepare' method in each expression.  The prepare method is intended to
     * mutate each expression so that references can be qualified, sub-queries
     * can be resolved, and variable substitutions can be substituted.
     */
    public void prepareAllExpressions(ExpressionPreparer preparer)
            throws DatabaseException {

        for (Object v : map.values()) {
            if (v != null) {
                prepareExpressionsInObject(v, preparer);
            }
        }

    }

    private void prepareExpressionsInObject(Object v,
                                            ExpressionPreparer preparer) throws DatabaseException {
        // If expression
        if (v instanceof Expression) {
            ((Expression) v).prepare(preparer);
        }
        // If an array of expression
        else if (v instanceof Expression[]) {
            Expression[] exp_list = (Expression[]) v;
            for (Expression expression : exp_list) {
                expression.prepare(preparer);
            }
        }
        // If a StatementTreeObject then can use the 'prepareExpressions' method.
        else if (v instanceof StatementTreeObject) {
            StatementTreeObject stob = (StatementTreeObject) v;
            stob.prepareExpressions(preparer);
        }
        // If a StatementTree then can use the prepareAllExpressions method.
        else if (v instanceof StatementTree) {
            StatementTree st = (StatementTree) v;
            st.prepareAllExpressions(preparer);
        }
        // If a list of objects,
        else if (v instanceof List) {
            List list = (List) v;
            for (Object ob : list) {
                prepareExpressionsInObject(ob, preparer);
            }
        }
    }

    /**
     * Clones a single object.
     */
    public static Object cloneSingleObject(Object entry)
            throws CloneNotSupportedException {

        // Immutable entries,
        if (entry == null ||
                entry instanceof TObject ||
                entry instanceof TType ||
                entry instanceof Boolean ||
                entry instanceof String ||
                entry instanceof BigDecimal ||
                entry instanceof Integer) {
            // Immutable entries
        } else if (entry instanceof Expression) {
            entry = ((Expression) entry).clone();
        } else if (entry instanceof Expression[]) {
            Expression[] exps = (Expression[]) ((Expression[]) entry).clone();
            // Clone each element of the array
            for (int n = 0; n < exps.length; ++n) {
                exps[n] = (Expression) exps[n].clone();
            }
            entry = exps;
        } else if (entry instanceof Variable) {
            entry = ((Variable) entry).clone();
        } else if (entry instanceof StatementTreeObject) {
            entry = ((StatementTreeObject) entry).clone();
        } else if (entry instanceof StatementTree) {
            entry = ((StatementTree) entry).clone();
        } else if (entry instanceof List) {
            // Clone the list by making a new ArrayList and adding a cloned version
            // of each element into it.
            List list = (List) entry;
            ArrayList cloned_list = new ArrayList(list.size());
            for (Object o : list) {
                cloned_list.add(cloneSingleObject(o));
            }
            entry = cloned_list;
        } else {
            throw new CloneNotSupportedException(entry.getClass().toString());
        }

        return entry;
    }

    /**
     * Performs a deep clone of this object, calling 'clone' on any elements
     * that are mutable or shallow copying immutable members.
     */
    public Object clone() throws CloneNotSupportedException {
        // Shallow clone first
        StatementTree v = (StatementTree) super.clone();
        // Clone the map
        HashMap cloned_map = new HashMap();
        v.map = cloned_map;

        // For each key, clone the entry
        for (Object key : map.keySet()) {
            Object entry = map.get(key);

            entry = cloneSingleObject(entry);

            cloned_map.put(key, entry);
        }

        return v;
    }

    /**
     * For diagnostic.
     */
    public String toString() {
        return "[ " + getClassName() + " [ " + map + " ] ]";
    }

}
