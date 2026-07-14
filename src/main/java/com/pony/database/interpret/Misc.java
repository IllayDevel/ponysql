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

import com.pony.database.*;

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
