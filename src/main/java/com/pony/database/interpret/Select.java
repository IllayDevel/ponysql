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
import com.pony.debug.*;
import com.pony.util.IntegerVector;

import java.util.Set;
import java.util.List;
import java.util.Vector;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Logic for interpreting an SQL SELECT statement.
 *
 * @author Tobias Downer
 */

public class Select extends Statement {

    /**
     * The TableSelectExpression representing the select query itself.
     */
    private TableSelectExpression select_expression;

    /**
     * The list of all columns to order by. (ByColumn)
     */
    private ArrayList order_by;

    /**
     * The list of columns in the 'order_by' clause fully resolved.
     */
    private Variable[] order_cols;

    /**
     * The plan for evaluating this select expression.
     */
    private QueryPlanNode plan;


    /**
     * Checks the permissions for this user to determine if they are allowed to
     * select (read) from tables in this plan.  If the user is not allowed to
     * select from a table in the plan, a UserAccessException is thrown.  This is
     * a static method.
     */
    static void checkUserSelectPermissions(
            DatabaseQueryContext context, User user, QueryPlanNode plan)
            throws DatabaseException {

        // Discover the list of TableName objects this query touches,
        ArrayList touched_tables = plan.discoverTableNames(new ArrayList());
        Database dbase = context.getDatabase();
        // Check that the user is allowed to select from these tables.
        for (int i = 0; i < touched_tables.size(); ++i) {
            TableName t = (TableName) touched_tables.get(i);
            if (!dbase.canUserSelectFromTableObject(context, user, t, null)) {
                throw new UserAccessException(
                        "User not permitted to select from table: " + t);
            }
        }
    }

    /**
     * Prepares the select statement with a Database object.  This sets up
     * internal state so that it correctly maps to a database.  Also, this
     * checks format to ensure there are no run-time syntax problems.  This must
     * be called because we 'evaluate' the statement.
     * <p>
     * NOTE: Care must be taken to ensure that all methods called here are safe
     *   in as far as modifications to the data occuring.  The rules for
     *   safety should be as follows.  If the database is in EXCLUSIVE mode,
     *   then we need to wait until it's switched back to SHARED mode
     *   before this method is called.
     *   All collection of information done here should not involve any table
     *   state info. except for column count, column names, column types, etc.
     *   Queries such as obtaining the row count, selectable scheme information,
     *   and certainly 'getCellContents' must never be called during prepare.
     *   When prepare finishes, the affected tables are locked and the query ia
     *   safe to 'evaluate' at which time table state is safe to inspect.
     */
    public void prepare() throws DatabaseException {
        DatabaseConnection db = database;

        // Prepare this object from the StatementTree,
        // The select expression itself
        select_expression =
                (TableSelectExpression) cmd.getObject("table_expression");
        // The order by information
        order_by = (ArrayList) cmd.getObject("order_by");

        // Generate the TableExpressionFromSet hierarchy for the expression,
        TableExpressionFromSet from_set =
                Planner.generateFromSet(select_expression, db);

        // Form the plan
        plan = Planner.formQueryPlan(db, select_expression, from_set, order_by);

    }


    /**
     * Evaluates the select statement with the given Database context.
     */
    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        // Check the permissions for this user to select from the tables in the
        // given plan.
        checkUserSelectPermissions(context, user, plan);

        boolean error = true;
        try {
            Table t = plan.evaluate(context);
            error = false;
            return t;
        } finally {
            // If an error occured, dump the query plan to the debug log.
            // Or just dump the query plan if debug level = INFORMATION
            if (Debug().isInterestedIn(Lvl.INFORMATION) ||
                    (error && Debug().isInterestedIn(Lvl.WARNING))) {
                StringBuffer buf = new StringBuffer();
                plan.debugString(0, buf);

                Debug().write(Lvl.WARNING, this,
                        "Query Plan debug:\n" +
                                buf.toString());
            }
        }

    }


    /**
     * Outputs information for debugging.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[ SELECT: expression=");
        buf.append(select_expression.toString());
        buf.append(" ORDER_BY=");
        buf.append(order_by);
        buf.append(" ]");
        return new String(buf);
    }

}
