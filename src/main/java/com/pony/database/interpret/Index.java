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

import com.pony.database.DataTableDef;
import com.pony.database.DatabaseException;
import com.pony.database.DatabaseQueryContext;
import com.pony.database.FunctionTable;
import com.pony.database.StatementException;
import com.pony.database.Table;
import com.pony.database.TableName;
import com.pony.database.UserAccessException;

import java.util.ArrayList;

/**
 * The logic of CREATE INDEX and DROP INDEX SQL commands.
 */
public class Index extends Statement {

    private String type;
    private String index_name;
    private TableName table_name;
    private String[] column_names;
    private boolean unique;

    public void prepare() throws DatabaseException {
        type = (String) cmd.getObject("type");
        index_name = (String) cmd.getObject("index_name");
        unique = cmd.getBoolean("unique");

        String raw_table_name = (String) cmd.getObject("table_name");
        table_name = resolveTableName(raw_table_name, database);

        if ("create".equals(type)) {
            ArrayList column_list = (ArrayList) cmd.getObject("column_list");
            column_names = resolveColumnNames(table_name, column_list);
        } else if (!"drop".equals(type)) {
            throw new DatabaseException("Unknown index operation: " + type);
        }
    }

    public Table evaluate() throws DatabaseException {
        DatabaseQueryContext context = new DatabaseQueryContext(database);

        if (!database.getDatabase().canUserAlterTableObject(context,
                user, table_name)) {
            throw new UserAccessException(
                    "User not permitted to alter table: " + table_name);
        }

        if ("create".equals(type)) {
            database.createIndex(table_name, index_name, column_names, unique);
        } else if ("drop".equals(type)) {
            database.dropIndex(table_name, index_name);
        } else {
            throw new StatementException("Unknown index operation: " + type);
        }

        return FunctionTable.resultTable(context, 0);
    }

    private String[] resolveColumnNames(TableName tableName, ArrayList columnList)
            throws DatabaseException {
        if (!database.tableExists(tableName)) {
            throw new DatabaseException("Table '" + tableName + "' does not exist.");
        }

        DataTableDef tableDef = database.getDataTableDef(tableName);
        String[] result = new String[columnList.size()];
        boolean ignoreCase = database.isInCaseInsensitiveMode();
        for (int i = 0; i < columnList.size(); ++i) {
            String columnName = columnList.get(i).toString();
            String found = null;
            for (int n = 0; n < tableDef.columnCount(); ++n) {
                String existing = tableDef.columnAt(n).getName();
                boolean matches = ignoreCase
                        ? existing.equalsIgnoreCase(columnName)
                        : existing.equals(columnName);
                if (matches) {
                    if (found != null) {
                        throw new DatabaseException(
                                "Ambiguous column name '" + columnName + "'");
                    }
                    found = existing;
                }
            }
            if (found == null) {
                throw new DatabaseException(
                        "Column '" + columnName + "' not found.");
            }
            result[i] = found;
        }
        return result;
    }

}
