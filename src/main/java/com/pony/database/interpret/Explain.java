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

package com.pony.database.interpret;

import com.pony.database.DataTableColumnDef;
import com.pony.database.Database;
import com.pony.database.DatabaseConnection;
import com.pony.database.DatabaseException;
import com.pony.database.DatabaseQueryContext;
import com.pony.database.QueryPlanNode;
import com.pony.database.StatementTree;
import com.pony.database.TObject;
import com.pony.database.Table;
import com.pony.database.TemporaryTable;

import java.util.ArrayList;

/**
 * Logic for interpreting the EXPLAIN SELECT statement.
 */
public class Explain extends Statement {

    private StatementTree select_statement;
    private TableSelectExpression select_expression;
    private ArrayList order_by;
    private Integer limit;
    private Integer offset;
    private QueryPlanNode plan;

    public void prepare() throws DatabaseException {
        DatabaseConnection db = database;

        select_statement = (StatementTree) cmd.getObject("select_statement");
        select_expression =
                (TableSelectExpression) select_statement.getObject("table_expression");
        order_by = (ArrayList) select_statement.getObject("order_by");
        limit = (Integer) select_statement.getInt("limit");
        offset = (Integer) select_statement.getInt("offset");

        TableExpressionFromSet from_set =
                Planner.generateFromSet(select_expression, db);
        plan = Planner.formQueryPlan(
                db, select_expression, from_set, order_by,
                Select.orderByLimit(offset, limit));
    }

    public Table evaluate() throws DatabaseException {
        DatabaseQueryContext context = new DatabaseQueryContext(database);

        Select.checkUserSelectPermissions(context, user, plan);

        StringBuffer plan_buffer = new StringBuffer();
        plan.debugString(0, plan_buffer);
        if (offset > 0 || limit >= 0) {
            plan_buffer.append("LIMIT/OFFSET: offset=");
            plan_buffer.append(offset);
            plan_buffer.append(", limit=");
            plan_buffer.append(limit);
            plan_buffer.append('\n');
        }

        Database d = database.getDatabase();
        DataTableColumnDef[] fields = new DataTableColumnDef[]{
                DataTableColumnDef.createStringColumn("plan")
        };
        TemporaryTable table = new TemporaryTable(d, "EXPLAIN", fields);
        table.newRow();
        table.setRowObject(TObject.stringVal(plan_buffer.toString()), 0);
        table.setupAllSelectableSchemes();
        return table;
    }

}
