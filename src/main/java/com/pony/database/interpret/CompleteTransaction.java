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
 * This represents either a COMMIT or ROLLBACK SQL command.
 *
 * @author Tobias Downer
 */

public class CompleteTransaction extends Statement {

    String command;  // This is set to either 'commit' or 'rollback'


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        command = (String) cmd.getObject("command");
    }

    public Table evaluate() throws DatabaseException, TransactionException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        if (command.equals("commit")) {
//      try {
            // Commit the current transaction on this connection.
            database.commit();
//      }
//      catch (TransactionException e) {
//        // This needs to be handled better!
//        Debug.writeException(e);
//        throw new DatabaseException(e.getMessage());
//      }
            return FunctionTable.resultTable(context, 0);
        } else if (command.equals("rollback")) {
            // Rollback the current transaction on this connection.
            database.rollback();
            return FunctionTable.resultTable(context, 0);
        } else {
            throw new Error("Unrecognised transaction completion command.");
        }

    }


}
