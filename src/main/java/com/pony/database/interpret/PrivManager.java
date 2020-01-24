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

import java.util.ArrayList;

/**
 * Handler for grant/revoke queries for setting up grant information in the
 * database.
 *
 * @author Tobias Downer
 */

public class PrivManager extends Statement {


    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        // Nothing to do here
    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        String command_type = (String) cmd.getObject("command");

        ArrayList priv_list = (ArrayList) cmd.getObject("priv_list");
        String priv_object = (String) cmd.getObject("priv_object");

        int grant_object;
        String grant_param;

        // Parse the priv object,
        if (priv_object.startsWith("T:")) {
            // Granting to a table object
            String table_name_str = priv_object.substring(2);
            TableName table_name = database.resolveTableName(table_name_str);
            // Check the table exists
            if (!database.tableExists(table_name)) {
                throw new DatabaseException("Table '" +
                        table_name + "' doesn't exist.");
            }
            grant_object = GrantManager.TABLE;
            grant_param = table_name.toString();
        } else if (priv_object.startsWith("S:")) {
            // Granting to a schema object
            String schema_name_str = priv_object.substring(2);
            SchemaDef schema_name = database.resolveSchemaName(schema_name_str);
            // Check the schema exists
            if (schema_name == null ||
                    !database.schemaExists(schema_name.toString())) {
                schema_name_str = schema_name == null ? schema_name_str :
                        schema_name.toString();
                throw new DatabaseException("Schema '" + schema_name_str +
                        "' doesn't exist.");
            }
            grant_object = GrantManager.SCHEMA;
            grant_param = schema_name.toString();
        } else {
            throw new Error("Priv object formatting error.");
        }

        if (command_type.equals("GRANT")) {
            ArrayList grant_to = (ArrayList) cmd.getObject("grant_to");
            boolean grant_option = cmd.getBoolean("grant_option");

            // Get the grant manager.
            GrantManager manager = context.getGrantManager();

            // Get the grant options this user has on the given object.
            Privileges options_privs = manager.userGrantOptions(
                    grant_object, grant_param, user.getUserName());

            // Is the user permitted to give out these privs?
            Privileges grant_privs = Privileges.EMPTY_PRIVS;
            for (Object item : priv_list) {
                String priv = ((String) item).toUpperCase();
                int priv_bit;
                if (priv.equals("ALL")) {
                    if (grant_object == GrantManager.TABLE) {
                        priv_bit = Privileges.TABLE_ALL_PRIVS.toInt();
                    } else if (grant_object == GrantManager.SCHEMA) {
                        priv_bit = Privileges.SCHEMA_ALL_PRIVS.toInt();
                    } else {
                        throw new Error("Unrecognised grant object.");
                    }
                } else {
                    priv_bit = Privileges.parseString(priv);
                }
                if (!options_privs.permits(priv_bit)) {
                    throw new UserAccessException(
                            "User is not permitted to grant '" + priv +
                                    "' access on object " + grant_param);
                }
                grant_privs = grant_privs.add(priv_bit);
            }

            // Do the users exist?
            for (Object value : grant_to) {
                String name = (String) value;
                if (!name.equalsIgnoreCase("public") &&
                        !database.getDatabase().userExists(context, name)) {
                    throw new DatabaseException("User '" + name + "' doesn't exist.");
                }
            }

            // Everything checks out so add the grants to the users.
            for (Object o : grant_to) {
                String name = (String) o;
                if (name.equalsIgnoreCase("public")) {
                    // Add a public grant,
                    manager.addGrant(grant_privs, grant_object, grant_param,
                            GrantManager.PUBLIC_USERNAME_STR,
                            grant_option, user.getUserName());
                } else {
                    // Add a user grant.
                    manager.addGrant(grant_privs, grant_object, grant_param,
                            name, grant_option, user.getUserName());
                }
            }

            // All done.

        } else if (command_type.equals("REVOKE")) {
            ArrayList revoke_from = (ArrayList) cmd.getObject("revoke_from");
            boolean revoke_grant_option = cmd.getBoolean("revoke_grant_option");

            // Get the grant manager.
            GrantManager manager = context.getGrantManager();

            // Is the user permitted to give out these privs?
            Privileges revoke_privs = Privileges.EMPTY_PRIVS;
            for (Object value : priv_list) {
                String priv = ((String) value).toUpperCase();
                int priv_bit;
                if (priv.equals("ALL")) {
                    if (grant_object == GrantManager.TABLE) {
                        priv_bit = Privileges.TABLE_ALL_PRIVS.toInt();
                    } else if (grant_object == GrantManager.SCHEMA) {
                        priv_bit = Privileges.SCHEMA_ALL_PRIVS.toInt();
                    } else {
                        throw new Error("Unrecognised grant object.");
                    }
                } else {
                    priv_bit = Privileges.parseString(priv);
                }
                revoke_privs = revoke_privs.add(priv_bit);
            }

            // Revoke the grants for the given users
            for (Object o : revoke_from) {
                String name = (String) o;
                if (name.equalsIgnoreCase("public")) {
                    // Revoke a public grant,
                    manager.removeGrant(revoke_privs, grant_object, grant_param,
                            GrantManager.PUBLIC_USERNAME_STR,
                            revoke_grant_option, user.getUserName());
                } else {
                    // Revoke a user grant.
                    manager.removeGrant(revoke_privs, grant_object, grant_param,
                            name, revoke_grant_option, user.getUserName());
                }
            }

            // All done.

        } else {
            throw new Error("Unknown priv manager command: " + command_type);
        }

        return FunctionTable.resultTable(context, 0);
    }


}

