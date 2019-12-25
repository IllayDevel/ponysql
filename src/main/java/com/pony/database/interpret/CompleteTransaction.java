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
