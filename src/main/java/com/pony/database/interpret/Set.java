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

/**
 * The SQL SET statement.  Sets properties within the current local database
 * connection such as auto-commit mode.
 *
 * @author Tobias Downer
 */

public class Set extends Statement {

    /**
     * The type of set this is.
     */
    String type;

    /**
     * The variable name of this set statement.
     */
    String var_name;

    /**
     * The Expression that is the value to assign the variable to
     * (if applicable).
     */
    Expression exp;

    /**
     * The value to assign the value to (if applicable).
     */
    String value;


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        type = (String) cmd.getObject("type");
        var_name = (String) cmd.getObject("var_name");
        exp = (Expression) cmd.getObject("exp");
        value = (String) cmd.getObject("value");
    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        String com = type.toLowerCase();

        switch (com) {
            case "varset":
                database.setVar(var_name, exp);
                break;
            case "isolationset":
                value = value.toLowerCase();
                database.setTransactionIsolation(value);
                break;
            case "autocommit":
                value = value.toLowerCase();
                if (value.equals("on") ||
                        value.equals("1")) {
                    database.setAutoCommit(true);
                } else if (value.equals("off") ||
                        value.equals("0")) {
                    database.setAutoCommit(false);
                } else {
                    throw new DatabaseException("Unrecognised value for SET AUTO COMMIT");
                }
                break;
            case "schema":
                // It's particularly important that this is done during exclusive
                // lock because SELECT requires the schema name doesn't change in
                // mid-process.

                // Change the connection to the schema
                database.setDefaultSchema(value);

                break;
            default:
                throw new DatabaseException("Unrecognised set command.");
        }

        return FunctionTable.resultTable(context, 0);

    }


}
