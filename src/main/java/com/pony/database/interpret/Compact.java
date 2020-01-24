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

package com.pony.database.interpret;

import com.pony.database.*;

/**
 * Statement that handles COMPACT sql command.
 *
 * @author Tobias Downer
 */

public class Compact extends Statement {

    /**
     * The name the table that we are to update.
     */
    String table_name;

    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        table_name = (String) cmd.getObject("table_name");
    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

//    TableName tname =
//                TableName.resolve(database.getCurrentSchema(), table_name);
        TableName tname = resolveTableName(table_name, database);
        // Does the table exist?
        if (!database.tableExists(tname)) {
            throw new DatabaseException("Table '" + tname + "' does not exist.");
        }

        // Does the user have privs to compact this tables?
        if (!database.getDatabase().canUserCompactTableObject(context,
                user, tname)) {
            throw new UserAccessException(
                    "User not permitted to compact table: " + table_name);
        }

        // Compact the table,
        database.compactTable(tname);

        // Return '0' if success.
        return FunctionTable.resultTable(context, 0);

    }


}
