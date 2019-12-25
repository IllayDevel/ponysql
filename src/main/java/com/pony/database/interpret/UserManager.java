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
 * Handler for User commands for creating, altering and dropping user accounts
 * in the database.
 *
 * @author Tobias Downer
 */

public class UserManager extends Statement {


    /**
     * Private method that sets the user groups and lock status.
     */
    private void internalSetUserGroupsAndLock(
            DatabaseQueryContext context, String username,
            Expression[] groups_list, String lock_status)
            throws DatabaseException {

        Database db = context.getDatabase();

        // Add the user to any groups
        if (groups_list != null) {
            // Delete all the groups the user currently belongs to
            db.deleteAllUserGroups(context, username);
            for (Expression expression : groups_list) {
                TObject group_tob = expression.evaluate(null, null, context);
                String group_str = group_tob.getObject().toString();
                db.addUserToGroup(context, username, group_str);
            }
        }

        // Do we lock this user?
        if (lock_status != null) {
            if (lock_status.equals("LOCK")) {
                db.setUserLock(context, user, true);
            } else {
                db.setUserLock(context, user, false);
            }
        }

    }

    /**
     * Private method that creates a new user.
     */
    private void internalCreateUser(
            DatabaseQueryContext context, String username, String password_str,
            Expression[] groups_list, String lock_status)
            throws DatabaseException {

        // Create the user
        Database db = context.getDatabase();
        db.createUser(context, username, password_str);

        internalSetUserGroupsAndLock(context, username, groups_list, lock_status);

        // Allow all localhost TCP connections.
        // NOTE: Permissive initial security!
        db.grantHostAccessToUser(context, username, "TCP", "%");
        // Allow all Local connections (from within JVM).
        db.grantHostAccessToUser(context, username, "Local", "%");

    }

    // ---------- Implemented from Statement ----------

    public void prepare() throws DatabaseException {
        // Nothing to do here
    }

    public Table evaluate() throws DatabaseException {

        DatabaseQueryContext context = new DatabaseQueryContext(database);

        String command_type = (String) cmd.getObject("type");
        String username = (String) cmd.getObject("username");

        // True if current user is altering their own user record.
        boolean modify_own_record = command_type.equals("ALTER USER") &&
                user.getUserName().equals(username);
        // True if current user is allowed to create and drop users.
        boolean secure_access_privs =
                context.getDatabase().canUserCreateAndDropUsers(context, user);

        // Does the user have permissions to do this?  They must be part of the
        // 'secure access' priv group or they are modifying there own record.
        if (!(modify_own_record || secure_access_privs)) {
            throw new DatabaseException(
                    "User is not permitted to create, alter or drop user.");
        }

        if (username.equalsIgnoreCase("public")) {
            throw new DatabaseException("Username 'public' is reserved.");
        }

        // Are we creating a new user?
        if (command_type.equals("CREATE USER") ||
                command_type.equals("ALTER USER")) {

            Expression password = (Expression) cmd.getObject("password_expression");
            Expression[] groups_list = (Expression[]) cmd.getObject("groups_list");
            String lock_status = (String) cmd.getObject("lock_status");

            String password_str = null;
            if (password != null) {
                TObject passwd_tob = password.evaluate(null, null, context);
                password_str = passwd_tob.getObject().toString();
            }

            if (command_type.equals("CREATE USER")) {
                // -- Creating a new user ---

                // First try and create the new user,
                Database db = context.getDatabase();
                if (!db.userExists(context, username)) {
                    internalCreateUser(context, username, password_str,
                            groups_list, lock_status);
                } else {
                    throw new DatabaseException(
                            "User '" + username + "' already exists.");
                }

            } else if (command_type.equals("ALTER USER")) {
                // -- Altering a user --

                // If we don't have secure access privs then we need to check that the
                // user is permitted to change the groups_list and lock_status.
                // Altering your own password is allowed, but you can't change the
                // groups you belong to, etc.
                if (!secure_access_privs) {
                    if (groups_list != null) {
                        throw new DatabaseException(
                                "User is not permitted to alter user groups.");
                    }
                    if (lock_status != null) {
                        throw new DatabaseException(
                                "User is not permitted to alter user lock status.");
                    }
                }

                Database db = context.getDatabase();
                if (db.userExists(context, username)) {
                    if (password_str != null) {
                        db.alterUserPassword(context, username, password_str);
                    }
                    internalSetUserGroupsAndLock(context, username,
                            groups_list, lock_status);
                } else {
                    throw new DatabaseException("User '" + username + "' doesn't exist.");
                }
            }

        } else if (command_type.equals("DROP USER")) {
            Database db = context.getDatabase();
            if (db.userExists(context, username)) {
                // Delete the user
                db.deleteUser(context, username);
            } else {
                throw new DatabaseException("User '" + username + "' doesn't exist.");
            }
        } else {
            throw new DatabaseException("Unknown user manager command: " +
                    command_type);
        }

        return FunctionTable.resultTable(context, 0);
    }


}

