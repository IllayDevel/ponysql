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

import java.util.List;

/**
 * A parsed state container for the 'CREATE TRIGGER' statement.
 *
 * @author Tobias Downer
 */

public class CreateTrigger extends Statement {

    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
    }

    public Table evaluate() throws DatabaseException {

        String trigger_name = (String) cmd.getObject("trigger_name");
        String type = (String) cmd.getObject("type");
        String table_name = (String) cmd.getObject("table_name");
        List types = (List) cmd.getObject("trigger_types");

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        TableName tname = TableName.resolve(database.getCurrentSchema(),
                table_name);

        if (type.equals("callback_trigger")) {
            // Callback trigger - notifies the client when an event on a table
            // occurs.
            if (types.size() > 1) {
                throw new DatabaseException(
                        "Multiple triggered types not allowed for callback triggers.");
            }

            String trig_type = ((String) types.get(0)).toUpperCase();
            int int_type;
            if (trig_type.equals("INSERT")) {
                int_type = TriggerEvent.INSERT;
            } else if (trig_type.equals("DELETE")) {
                int_type = TriggerEvent.DELETE;
            } else if (trig_type.equals("UPDATE")) {
                int_type = TriggerEvent.UPDATE;
            } else {
                throw new DatabaseException("Unknown trigger type: " + trig_type);
            }

            database.createTrigger(trigger_name, tname.toString(), int_type);

        } else if (type.equals("procedure_trigger")) {

            // Get the procedure manager
            ProcedureManager proc_manager = database.getProcedureManager();

            String before_after = (String) cmd.getObject("before_after");
            String procedure_name = (String) cmd.getObject("procedure_name");
            Expression[] procedure_args =
                    (Expression[]) cmd.getObject("procedure_args");

            // Convert the trigger into a table name,
            String schema_name = database.getCurrentSchema();
            TableName t_name = TableName.resolve(schema_name, trigger_name);
            t_name = database.tryResolveCase(t_name);

            // Resolve the procedure name into a TableName object.
            TableName t_p_name = TableName.resolve(schema_name, procedure_name);
            t_p_name = database.tryResolveCase(t_p_name);

            // Does the procedure exist in the system schema?
            ProcedureName p_name = new ProcedureName(t_p_name);

            // Check the trigger name doesn't clash with any existing database object.
            if (database.tableExists(t_name)) {
                throw new DatabaseException("A database object with name '" + t_name +
                        "' already exists.");
            }

            // Check the procedure exists.
            if (!proc_manager.procedureExists(p_name)) {
                throw new DatabaseException("Procedure '" + p_name +
                        "' could not be found.");
            }

            // Resolve the listening type
            int listen_type = 0;
            if (before_after.equals("before")) {
                listen_type |= TableModificationEvent.BEFORE;
            } else if (before_after.equals("after")) {
                listen_type |= TableModificationEvent.AFTER;
            } else {
                throw new RuntimeException("Unknown before/after type.");
            }

            for (int i = 0; i < types.size(); ++i) {
                String trig_type = (String) types.get(i);
                if (trig_type.equals("insert")) {
                    listen_type |= TableModificationEvent.INSERT;
                } else if (trig_type.equals("delete")) {
                    listen_type |= TableModificationEvent.DELETE;
                } else if (trig_type.equals("update")) {
                    listen_type |= TableModificationEvent.UPDATE;
                }
            }

            // Resolve the procedure arguments,
            TObject[] vals = new TObject[procedure_args.length];
            for (int i = 0; i < procedure_args.length; ++i) {
                vals[i] = procedure_args[i].evaluate(null, null, context);
            }

            // Create the trigger,
            ConnectionTriggerManager manager = database.getConnectionTriggerManager();
            manager.createTableTrigger(t_name.getSchema(), t_name.getName(),
                    listen_type, tname, p_name.toString(), vals);

            // The initial grants for a trigger is to give the user who created it
            // full access.
            database.getGrantManager().addGrant(
                    Privileges.PROCEDURE_ALL_PRIVS, GrantManager.TABLE,
                    t_name.toString(), user.getUserName(), true,
                    Database.INTERNAL_SECURE_USERNAME);

        } else {
            throw new RuntimeException("Unknown trigger type.");
        }

        // Return success
        return FunctionTable.resultTable(context, 0);
    }


}
