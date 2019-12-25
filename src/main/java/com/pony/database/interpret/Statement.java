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

import com.pony.database.jdbc.SQLQuery;
import com.pony.database.*;
import com.pony.debug.DebugLogger;

import java.util.*;

/**
 * Provides a set of useful utility functions to use by all the
 * interpretted statements.
 *
 * @author Tobias Downer
 */

public abstract class Statement {

    /**
     * The Database context.
     */
    protected DatabaseConnection database;

    /**
     * The user context.
     */
    protected User user;

    /**
     * The StatementTree object that is the container for the query.
     */
    protected StatementTree cmd;

    /**
     * The SQLQuery object that was used to produce this statement.
     */
    protected SQLQuery query;

    /**
     * The list of all FromTableInterface objects of resources referenced in
     * this query.  (FromTableInterface)
     */
    protected Vector table_list = new Vector();

    /**
     * Returns a DebugLogger object used to log debug commands.
     */
    public final DebugLogger Debug() {
        return database.Debug();
    }

    /**
     * Resets this statement so it may be re-prepared and evaluated again.
     * Useful for repeating a query multiple times.
     */
    void reset() {
        database = null;
        user = null;
        table_list = new Vector();
    }

    /**
     * Performs initial preparation on the contents of the StatementTree by
     * resolving all sub queries and mapping functions to their executable
     * forms.
     * <p>
     * Given a StatementTree and a Database context, this method will convert
     * all sub-queries found in the StatementTree to a Queriable object.  In
     * other words, all StatementTree are converted to Select objects.  The given
     * 'database' object is used as the session to prepare the sub-queries
     * against.
     * <p>
     * This is called after 'init' and before 'prepare'.
     */
    public final void resolveTree() throws DatabaseException {

        // For every expression in this select we must go through and resolve
        // any sub-queries we find to the correct Select object.
        // This method will prepare the sub-query substitute the StatementTree
        // object for a Select object in the expression.
        ExpressionPreparer preparer = new ExpressionPreparer() {
            public boolean canPrepare(Object element) {
                return element instanceof StatementTree;
            }

            public Object prepare(Object element) throws DatabaseException {
                StatementTree stmt_tree = (StatementTree) element;
                Select stmt = new Select();
                stmt.init(database, stmt_tree, null);
                stmt.resolveTree();
                stmt.prepare();
                return stmt;
            }
        };
        cmd.prepareAllExpressions(preparer);

    }

    /**
     * Given a fully resolved table name ( eg. Part.id ) this method will
     * attempt to find the Table object that the column is in.
     */
    FromTableInterface findTableWithColumn(Variable column_name) {
        for (int i = 0; i < table_list.size(); ++i) {
            FromTableInterface table = (FromTableInterface) table_list.elementAt(i);
            TableName tname = column_name.getTableName();
            String sch_name = null;
            String tab_name = null;
            String col_name = column_name.getName();
            if (tname != null) {
                sch_name = tname.getSchema();
                tab_name = tname.getName();
            }
            int rcc = table.resolveColumnCount(null, sch_name, tab_name, col_name);
            if (rcc > 0) {
                return table;
            }
        }
        return null;
    }

    /**
     * Given a fully resolved table name ( eg. Part.id ) this returns true if
     * there is a table with the given column name, otherwise false.
     * <p>
     * NOTE: Intended to be overwritten...
     */
    boolean existsTableWithColumn(Variable column_name) {
        return findTableWithColumn(column_name) != null;
    }

    /**
     * Overwrite this method if your statement has some sort of column aliasing
     * capability (such as a select statement).  Returns a list of all fully
     * qualified Variables that match the alias name, or an empty list if no
     * matches found.
     * <p>
     * By default, returns an empty list.
     */
    ArrayList resolveAgainstAliases(Variable alias_name) {
        return new ArrayList(0);
    }

    /**
     * Resolves a TableName string (eg. 'Customer' 'APP.Customer' ) to a
     * TableName object.  If the schema part of the table name is not present
     * then it is set to the current schema of the database connection.  If the
     * database is ignoring the case then this will correctly resolve the table
     * to the cased version of the table name.
     */
    TableName resolveTableName(String name, DatabaseConnection db) {
        return db.resolveTableName(name);
    }

    /**
     * Returns the first FromTableInterface object that matches the given schema,
     * table reference.  Returns null if no objects with the given schema/name
     * reference match.
     */
    FromTableInterface findTableInQuery(String schema, String name) {
        for (Object o : table_list) {
            FromTableInterface table = (FromTableInterface) o;
            if (table.matchesReference(null, schema, name)) {
                return table;
            }
        }
        return null;
    }

    /**
     * Attempts to resolve an ambiguous column name such as 'id' into a
     * Variable from the tables in this statement.
     */
    Variable resolveColumn(Variable v) {
        // Try and resolve against alias names first,
        ArrayList list = new ArrayList();
        list.addAll(resolveAgainstAliases(v));

        TableName tname = v.getTableName();
        String sch_name = null;
        String tab_name = null;
        String col_name = v.getName();
        if (tname != null) {
            sch_name = tname.getSchema();
            tab_name = tname.getName();
        }

        int matches_found = 0;
        // Find matches in our list of tables sources,
        for (int i = 0; i < table_list.size(); ++i) {
            FromTableInterface table = (FromTableInterface) table_list.elementAt(i);
            int rcc = table.resolveColumnCount(null, sch_name, tab_name, col_name);
            if (rcc == 1) {
                Variable matched =
                        table.resolveColumn(null, sch_name, tab_name, col_name);
                list.add(matched);
            } else if (rcc > 1) {
                throw new StatementException("Ambiguous column name (" + v + ")");
            }
        }

        int total_matches = list.size();
        if (total_matches == 0) {
            throw new StatementException("Can't find column: " + v);
        } else if (total_matches == 1) {
            return (Variable) list.get(0);
        } else if (total_matches > 1) {
            // if there more than one match, check if they all match the identical
            // resource,
            throw new StatementException("Ambiguous column name (" + v + ")");
        } else {
            // Should never reach here but we include this exception to keep the
            // compiler happy.
            throw new Error("Negative total matches?");
        }

    }

    /**
     * Given a Variable object, this will resolve the name into a column name
     * the database understands (substitutes aliases, etc).
     */
    public Variable resolveVariableName(Variable v) {
        return resolveColumn(v);
    }

    /**
     * Given an Expression, this will run through the expression and resolve
     * any variable names via the 'resolveVariableName' method here.
     */
    void resolveExpression(Expression exp) {
        // NOTE: This gets variables in all function parameters.
        List vars = exp.allVariables();
        for (Object var : vars) {
            Variable v = (Variable) var;
            Variable to_set = resolveVariableName(v);
            v.set(to_set);
        }
    }

    /**
     * Add an FromTableInterface that is used within this query.  These tables
     * are used when we try to resolve a column name.
     */
    protected void addTable(FromTableInterface table) {
        table_list.addElement(table);
    }

    /**
     * Sets up internal variables for this statement for derived classes to use.
     * This is called before 'prepare' and 'isExclusive' is called.
     * <p>
     * It is assumed that any ? style parameters in the StatementTree will have
     * been resolved previous to a call to this method.
     *
     * @param db the DatabaseConnection that will execute this statement.
     * @param stree the StatementTree that contains the parsed content of the
     *   statement being executed.
     */
    public final void init(DatabaseConnection db, StatementTree stree,
                           SQLQuery query) {
        this.database = db;
        this.user = db.getUser();
        this.cmd = stree;
        this.query = query;
    }

    /**
     * Prepares the statement with the given Database object.  This is called
     * before the statement is evaluated.  The prepare statement queries the
     * database and resolves information about the statement (for example, it
     * resolves column names and aliases and determines the tables that are
     * touched by this statement so we can lock the appropriate tables before
     * we evaluate).
     * <p>
     * NOTE: Care must be taken to ensure that all methods called here are safe
     *   in as far as modifications to the data occuring.  The rules for
     *   safety should be as follows.  If the database is in EXCLUSIVE mode,
     *   then we need to wait until it's switched back to SHARED mode before
     *   this method is called.
     *   All collection of information done here should not involve any table
     *   state info. except for column count, column names, column types, etc.
     *   Queries such as obtaining the row count, selectable scheme information,
     *   and certainly 'getCellContents' must never be called during prepare.
     *   When prepare finishes, the affected tables are locked and the query is
     *   safe to 'evaluate' at which time table state is safe to inspect.
     */
    public abstract void prepare() throws DatabaseException;

    /**
     * Evaluates the statement and returns a table that represents the result
     * set.  This is called after 'prepare'.
     */
    public abstract Table evaluate()
            throws DatabaseException, TransactionException;

}
