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
 * A parsed state container for the 'DROP TRIGGER' statement.
 *
 * @author Tobias Downer
 */

public class DropTrigger extends Statement {

    /**
     * The name of this trigger.
     */
    String trigger_name;


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        trigger_name = (String) cmd.getObject("trigger_name");
    }

    public Table evaluate() throws DatabaseException {

        String type = (String) cmd.getObject("type");

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        if (type.equals("callback_trigger")) {
            database.deleteTrigger(trigger_name);
        } else {

            // Convert the trigger into a table name,
            String schema_name = database.getCurrentSchema();
            TableName t_name = TableName.resolve(schema_name, trigger_name);
            t_name = database.tryResolveCase(t_name);

            ConnectionTriggerManager manager = database.getConnectionTriggerManager();
            manager.dropTrigger(t_name.getSchema(), t_name.getName());

            // Drop the grants for this object
            database.getGrantManager().revokeAllGrantsOnObject(
                    GrantManager.TABLE, t_name.toString());
        }

        // Return '0' if we created the trigger.
        return FunctionTable.resultTable(context, 0);
    }


}
