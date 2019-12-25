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

import com.pony.database.*;
import com.pony.database.jdbc.SQLQuery;
import com.pony.database.sql.SQL;
import com.pony.database.sql.ParseException;

import java.io.StringReader;
import java.sql.SQLException;

/**
 * An object used to execute SQL queries against a given DatabaseConnection
 * object.  The object maintains an SQL parser object as state which is
 * reused as necessary.
 * <p>
 * This object is a convenient way to execute SQL queries.
 *
 * @author Tobias Downer
 */

public class SQLQueryExecutor {

    /**
     * The SQL parser state.
     */
    private static final SQL sql_parser;

    static {
        // Set up the sql parser.
        sql_parser = new SQL(new StringReader(""));
    }

    /**
     * Constructs the executor.
     */
    public SQLQueryExecutor() {
    }

    /**
     * Executes the given SQLQuery object on the given DatabaseConnection object.
     * Returns a Table object that contains the result of the execution.
     * <p>
     * Note that this method does not perform any locking.  Any locking must have
     * happened before this method is called.
     * <p>
     * Also note that the returned Table object is onld valid within the
     * life-time of the lock unless the root lock requirements are satisified.
     */
    public Table execute(DatabaseConnection connection, SQLQuery query)
            throws SQLException, DatabaseException, TransactionException,
            ParseException {

        // StatementTree caching

        // Create a new parser and set the parameters...
        String query_str = query.getQuery();
        StatementTree statement_tree = null;
        StatementCache statement_cache =
                connection.getSystem().getStatementCache();

        if (statement_cache != null) {
            // Is this query cached?
            statement_tree = statement_cache.get(query_str);
        }
        if (statement_tree == null) {
            synchronized (sql_parser) {
                sql_parser.ReInit(new StringReader(query_str));
                sql_parser.reset();
                // Parse the statement.
                statement_tree = sql_parser.Statement();
            }
            // Put the statement tree in the cache
            if (statement_cache != null) {
                statement_cache.put(query_str, statement_tree);
            }
        }

        // Substitute all parameter substitutions in the statement tree.
        final Object[] vars = query.getVars();
        ExpressionPreparer preparer = new ExpressionPreparer() {
            public boolean canPrepare(Object element) {
                return (element instanceof ParameterSubstitution);
            }

            public Object prepare(Object element) {
                ParameterSubstitution ps = (ParameterSubstitution) element;
                int param_id = ps.getID();
                return TObject.objectVal(vars[param_id]);
            }
        };
        statement_tree.prepareAllExpressions(preparer);

        // Convert the StatementTree to a statement object
        Statement statement;
        String statement_class = statement_tree.getClassName();
        try {
            Class c = Class.forName(statement_class);
            statement = (Statement) c.newInstance();
        } catch (ClassNotFoundException e) {
            throw new SQLException(
                    "Could not find statement class: " + statement_class);
        } catch (InstantiationException e) {
            throw new SQLException(
                    "Could not instantiate class: " + statement_class);
        } catch (IllegalAccessException e) {
            throw new SQLException(
                    "Could not access class: " + statement_class);
        }


        // Initialize the statement
        statement.init(connection, statement_tree, query);

        // Automated statement tree preparation
        statement.resolveTree();

        // Prepare the statement.
        statement.prepare();

        // Evaluate the SQL statement.
        Table result = statement.evaluate();

        return result;

    }

}
