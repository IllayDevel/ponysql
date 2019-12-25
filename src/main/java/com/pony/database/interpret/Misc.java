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

import java.util.ArrayList;
import java.util.List;

/**
 * Misc statements that I couldn't be bothered to roll a new Statement class
 * for.  These have to be exceptional statements that do not read or write
 * to any tables and run in exclusive mode.
 *
 * @author Tobias Downer
 */

public class Misc extends Statement {

    /**
     * Set to true if this statement is a shutdown statement.
     */
    boolean shutdown = false;


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        Object command = cmd.getObject("command");
        shutdown = command.equals("shutdown");
    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        // Is this a shutdown statement?
        if (shutdown == true) {

            // Check the user has privs to shutdown...
            if (!database.getDatabase().canUserShutDown(context, user)) {
                throw new UserAccessException(
                        "User not permitted to shut down the database.");
            }

            // Shut down the database system.
            database.getDatabase().startShutDownThread();

            // Return 0 to indicate we going to be closing shop!
            return FunctionTable.resultTable(context, 0);

        }

        return FunctionTable.resultTable(context, 0);
    }


}
