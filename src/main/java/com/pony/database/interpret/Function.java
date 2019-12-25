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
 * A handler for defining and dropping functions.
 *
 * @author Tobias Downer
 */

public class Function extends Statement {

    /**
     * The type of command we are running through this Function object.
     */
    private String type;

    /**
     * The name of the function.
     */
    private TableName fun_name;

    // ----------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        type = (String) cmd.getObject("type");
        String function_name = (String) cmd.getObject("function_name");

        // Resolve the function name into a TableName object.
        String schema_name = database.getCurrentSchema();
        fun_name = TableName.resolve(schema_name, function_name);
        fun_name = database.tryResolveCase(fun_name);

    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        // Does the schema exist?
        boolean ignore_case = database.isInCaseInsensitiveMode();
        SchemaDef schema =
                database.resolveSchemaCase(fun_name.getSchema(), ignore_case);
        if (schema == null) {
            throw new DatabaseException("Schema '" + fun_name.getSchema() +
                    "' doesn't exist.");
        } else {
            fun_name = new TableName(schema.getName(), fun_name.getName());
        }

        if (type.equals("create")) {

            // Does the user have privs to create this function?
            if (!database.getDatabase().canUserCreateProcedureObject(context,
                    user, fun_name)) {
                throw new UserAccessException(
                        "User not permitted to create function: " + fun_name);
            }

            // Does a table already exist with this name?
            if (database.tableExists(fun_name)) {
                throw new DatabaseException("Database object with name '" + fun_name +
                        "' already exists.");
            }

            // Get the information about the function we are creating
            List arg_names = (List) cmd.getObject("arg_names");
            List arg_types = (List) cmd.getObject("arg_types");
            TObject loc_name = (TObject) cmd.getObject("location_name");
            TType return_type = (TType) cmd.getObject("return_type");

            // Note that we currently ignore the arg_names list.


            // Convert arg types to an array
            TType[] arg_type_array =
                    (TType[]) arg_types.toArray(new TType[arg_types.size()]);

            // We must parse the location name into a class name, and method name
            String java_specification = loc_name.getObject().toString();
            // Resolve the java_specification to an invokation method.
            java.lang.reflect.Method proc_method =
                    ProcedureManager.javaProcedureMethod(java_specification,
                            arg_type_array);
            if (proc_method == null) {
                throw new DatabaseException("Unable to find invokation method for " +
                        "Java stored procedure name: " + java_specification);
            }

            // Convert the information into an easily digestible form.
            ProcedureName proc_name = new ProcedureName(fun_name);
            int sz = arg_types.size();
            TType[] arg_list = new TType[sz];
            for (int i = 0; i < sz; ++i) {
                arg_list[i] = (TType) arg_types.get(i);
            }

            // Create the (Java) function,
            ProcedureManager manager = database.getProcedureManager();
            manager.defineJavaProcedure(proc_name, java_specification,
                    return_type, arg_list, user.getUserName());

            // The initial grants for a procedure is to give the user who created it
            // full access.
            database.getGrantManager().addGrant(
                    Privileges.PROCEDURE_ALL_PRIVS, GrantManager.TABLE,
                    proc_name.toString(), user.getUserName(), true,
                    Database.INTERNAL_SECURE_USERNAME);

        } else if (type.equals("drop")) {

            // Does the user have privs to create this function?
            if (!database.getDatabase().canUserDropProcedureObject(context,
                    user, fun_name)) {
                throw new UserAccessException(
                        "User not permitted to drop function: " + fun_name);
            }

            // Drop the function
            ProcedureName proc_name = new ProcedureName(fun_name);
            ProcedureManager manager = database.getProcedureManager();
            manager.deleteProcedure(proc_name);

            // Drop the grants for this object
            database.getGrantManager().revokeAllGrantsOnObject(
                    GrantManager.TABLE, proc_name.toString());

        } else {
            throw new RuntimeException("Unknown type: " + type);
        }

        // Return an update result table.
        return FunctionTable.resultTable(context, 0);

    }

}

