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

package com.pony.database;

import java.sql.*;
import java.io.File;
import java.io.PrintStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import com.pony.debug.*;
import com.pony.util.Log;
import com.pony.util.Stats;
import com.pony.database.global.*;
import com.pony.database.control.DefaultDBConfig;
import com.pony.database.jdbc.MSQLException;
import com.pony.store.Store;
import com.pony.store.MutableArea;

/**
 * The representation of a single database in the system.  A database
 * is a set of schema, a set of tables and table definitions of tables in
 * the schema, and a description of the schema.
 * <p>
 * This class encapsulates the top level behaviour of a database.  That is
 * of creating itself, initializing itself, shutting itself down, deleting
 * itself, creating/dropping a table, updating a table.  It is not the
 * responsibility of this class to handle table behaviour above this.  Top
 * level table behaviour is handled by DataTable through the DatabaseConnection
 * interface.
 * <p>
 * The Database object is also responsible for various database management
 * functions such a creating, editing and removing users, triggers, functions
 * and services.
 *
 * @author Tobias Downer
 */

public final class Database implements DatabaseConstants {

    // ---------- Statics ----------

    /**
     * The name of the system schema that contains tables refering to system
     * information.
     */
    public static final String SYSTEM_SCHEMA =
            TableDataConglomerate.SYSTEM_SCHEMA;

    /**
     * The name of the default schema.
     */
    public static final String DEFAULT_SCHEMA = "APP";

    /**
     * The name of the schema that contains JDBC helper tables.
     */
    public static final String JDBC_SCHEMA = "SYS_JDBC";

    /**
     * The password privs and grants table.
     */
    public static final TableName SYS_PASSWORD =
            new TableName(SYSTEM_SCHEMA, "Password");

    public static final TableName SYS_USERCONNECT =
            new TableName(SYSTEM_SCHEMA, "UserConnectPriv");

    public static final TableName SYS_USERPRIV =
            new TableName(SYSTEM_SCHEMA, "UserPriv");

    public static final TableName SYS_GRANTS =
            new TableName(SYSTEM_SCHEMA, "Grant");

    /**
     * The services table.
     */
    public static final TableName SYS_SERVICE =
            new TableName(SYSTEM_SCHEMA, "Service");

    /**
     * The function factory table.
     */
    public static final TableName SYS_FUNCTIONFACTORY =
            new TableName(SYSTEM_SCHEMA, "FunctionFactory");

    /**
     * The function table.
     */
    public static final TableName SYS_FUNCTION =
            new TableName(SYSTEM_SCHEMA, "Function");

    /**
     * The view table.
     */
    public static final TableName SYS_VIEW =
            new TableName(SYSTEM_SCHEMA, "View");

    /**
     * The label table.
     */
    public static final TableName SYS_LABEL =
            new TableName(SYSTEM_SCHEMA, "Label");

    /**
     * The system internally generated 'TableColumns' table.
     */
    public static final TableName SYS_TABLE_COLUMNS =
            new TableName(SYSTEM_SCHEMA, "TableColumns");

    /**
     * The system internally generated 'TableInfo' table.
     */
    public static final TableName SYS_TABLE_INFO =
            new TableName(SYSTEM_SCHEMA, "TableInfo");

    /**
     * The system internally generated 'DataTrigger' table.
     */
    public static final TableName SYS_DATA_TRIGGER =
            new TableName(SYSTEM_SCHEMA, "DataTrigger");

    /**
     * The system internally generated 'DatabaseStatistics' table.
     */
    public static final TableName SYS_DB_STATISTICS =
            new TableName(SYSTEM_SCHEMA, "DatabaseStatistics");

    /**
     * The OLD table used inside a triggered procedure to represent a triggered
     * row before the operation occurs.
     */
    public static final TableName OLD_TRIGGER_TABLE =
            new TableName(SYSTEM_SCHEMA, "OLD");

    /**
     * The NEW table used inside a triggered procedure to represent a triggered
     * row after the operation occurs.
     */
    public static final TableName NEW_TRIGGER_TABLE =
            new TableName(SYSTEM_SCHEMA, "NEW");


    /**
     * The name of the lock group.  If a user belongs to this group the user
     * account is locked and they are not allowed to log into the database.
     */
    public static final String LOCK_GROUP = "#locked";

    /**
     * THe name of the secure access group.  If a user belongs to this group they
     * are permitted to perform a number of priviledged operations such as
     * shutting down the database, and adding and removing users.
     */
    public static final String SECURE_GROUP = "secure access";

    /**
     * The name of the user manager group.  Users that belong in this group can
     * create, alter and drop users from the system.
     */
    public static final String USER_MANAGER_GROUP = "user manager";

    /**
     * The name of the schema manager group.  Users that belong in this group can
     * create and drop schema from the system.
     */
    public static final String SCHEMA_MANAGER_GROUP = "schema manager";


    /**
     * The username of the internal secure user.  The internal secure user is only
     * used for internal highly privileged operations.  This user is given full
     * privs to everything and is used to manage the system tables, for
     * authentication, etc.
     */
    public static final String INTERNAL_SECURE_USERNAME = "@SYSTEM";


    // ---------- Members ----------

    /**
     * The DatabaseSystem that this database is part of.
     */
    private final DatabaseSystem system;

    /**
     * The name of this database.
     */
    private final String name;

    /**
     * The TableDataConglomerate that contains the conglomerate of tables for
     * this database.
     */
    private final TableDataConglomerate conglomerate;

    /**
     * A flag which, when set to true, will cause the engine to delete the
     * database from the file system when it is shut down.
     */
    private boolean delete_on_shutdown;

    /**
     * An internal secure User that is given full grant access to the entire
     * database.  This user is used to execute system level queries such as
     * creating and updating system tables.
     */
    private final User internal_system_user;

    /**
     * The database wide TriggerManager object that dispatches trigger events
     * to the DatabaseConnection objects that are listening for the events.
     */
    private final TriggerManager trigger_manager;


    /**
     * This log file records the DQL commands executed on the server.
     */
    private Log commands_log;

    /**
     * This is set to true when the 'init()' method is first called.
     */
    private boolean initialised = false;

    /**
     * A table that has a single row but no columns.
     */
    private final Table SINGLE_ROW_TABLE;

    /**
     * The Constructor.  This takes a directory path in which the database is
     * stored.
     */
    public Database(DatabaseSystem system, String name) {
        this.system = system;
        this.delete_on_shutdown = false;
        this.name = name;
        conglomerate = new TableDataConglomerate(system, system.storeSystem());
        internal_system_user =
                new User(INTERNAL_SECURE_USERNAME, this, "", System.currentTimeMillis());

        // Create the single row table
        TemporaryTable t;
        t = new TemporaryTable(this,
                "SINGLE_ROW_TABLE", new DataTableColumnDef[0]);
        t.newRow();
        SINGLE_ROW_TABLE = t;

        trigger_manager = new TriggerManager(system);

    }

    /**
     * Returns the name of this database.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns true if this database is in read only mode.
     */
    public boolean isReadOnly() {
        return getSystem().readOnlyAccess();
    }

    /**
     * Returns the internal system user for this database.
     */
    private User internalSystemUser() {
        return internal_system_user;
    }

    // ---------- Log accesses ----------

    /**
     * Returns the log file where commands are recorded.
     */
    public Log getCommandsLog() {
        return commands_log;
    }

    /**
     * Returns the conglomerate for this database.
     */
    TableDataConglomerate getConglomerate() {
        return conglomerate;
    }

    /**
     * Returns a new DatabaseConnection instance that is used against this
     * database.
     * <p>
     * When a new connection is made on this database, this method is called
     * to create a new DatabaseConnection instance for the connection.  This
     * connection handles all transactional queries and modifications to the
     * database.
     */
    public DatabaseConnection createNewConnection(
            User user, DatabaseConnection.CallBack call_back) {
        if (user == null) {
            user = internalSystemUser();
        }

        DatabaseConnection connection =
                new DatabaseConnection(this, user, call_back);
        // Initialize the connection
        connection.init();

        return connection;
    }

    // ---------- Database user management functions ----------

    /**
     * Tries to authenticate a username/password against this database.  If we
     * fail to authenticate then a 'null' object is returned, otherwise a valid
     * User object is returned.  If a valid object is returned, the user
     * will be logged into the engine via the UserManager object (in
     * DatabaseSystem).  The developer must ensure that 'close' is called before
     * the object is disposed (logs out of the system).
     * <p>
     * This method also returns null if a user exists but was denied access from
     * the given host string.  The given 'host_name' object is formatted in the
     * database host connection encoding.  This method checks all the values
     * from the UserConnectPriv table for this user for the given protocol.
     * It first checks if the user is specifically DENIED access from the given
     * host.  It then checks if the user is ALLOWED access from the given host.
     * If a host is neither allowed or denied then it is denied.
     */
    public User authenticateUser(String username, String password,
                                 String connection_string) {

        // Create a temporary connection for authentication only...
        DatabaseConnection connection = createNewConnection(null, null);
        DatabaseQueryContext context = new DatabaseQueryContext(connection);
        connection.setCurrentSchema(SYSTEM_SCHEMA);
        LockingMechanism locker = connection.getLockingMechanism();
        locker.setMode(LockingMechanism.EXCLUSIVE_MODE);
        try {

            try {
                Connection jdbc = connection.getJDBCConnection();

                // Is the username/password in the database?
                PreparedStatement stmt = jdbc.prepareStatement(
                        " SELECT \"UserName\" FROM \"Password\" " +
                                "  WHERE \"Password.UserName\" = ? " +
                                "    AND \"Password.Password\" = ? ");
                stmt.setString(1, username);
                stmt.setString(2, password);
                ResultSet rs = stmt.executeQuery();
                if (!rs.next()) {
                    return null;
                }
                rs.close();
                stmt.close();

                // Now check if this user is permitted to connect from the given
                // host.
                if (userAllowedAccessFromHost(context,
                        username, connection_string)) {
                    // Successfully authenticated...
                    User user = new User(username, this,
                            connection_string, System.currentTimeMillis());
                    // Log the authenticated user in to the engine.
                    system.getUserManager().userLoggedIn(user);
                    return user;
                }

                return null;

            } catch (SQLException e) {
                if (e instanceof MSQLException) {
                    MSQLException msqle = (MSQLException) e;
                    Debug().write(Lvl.ERROR, this,
                            msqle.getServerErrorStackTrace());
                }
                Debug().writeException(Lvl.ERROR, e);
                throw new RuntimeException("SQL Error: " + e.getMessage());
            }

        } finally {
            try {
                // Make sure we commit the connection.
                connection.commit();
            } catch (TransactionException e) {
                // Just issue a warning...
                Debug().writeException(Lvl.WARNING, e);
            } finally {
                // Guarentee that we unluck from EXCLUSIVE
                locker.finishMode(LockingMechanism.EXCLUSIVE_MODE);
            }
            // And make sure we close (dispose) of the temporary connection.
            connection.close();
        }

    }

    /**
     * Performs check to determine if user is allowed access from the given
     * host.  See the comments of 'authenticateUser' for a description of
     * how this is determined.
     */
    private boolean userAllowedAccessFromHost(DatabaseQueryContext context,
                                              String username, String connection_string) {

        // The system user is not allowed to login
        if (username.equals(INTERNAL_SECURE_USERNAME)) {
            return false;
        }

        // We always allow access from 'Internal/*' (connections from the
        // 'getConnection' method of a com.pony.database.control.DBSystem object)
        // ISSUE: Should we add this as a rule?
        if (connection_string.startsWith("Internal/")) {
            return true;
        }

        // What's the protocol?
        int protocol_host_deliminator = connection_string.indexOf("/");
        String protocol =
                connection_string.substring(0, protocol_host_deliminator);
        String host = connection_string.substring(protocol_host_deliminator + 1);

        if (Debug().isInterestedIn(Lvl.INFORMATION)) {
            Debug().write(Lvl.INFORMATION, this,
                    "Checking host: protocol = " + protocol +
                            ", host = " + host);
        }

        // The table to check
        DataTable connect_priv = context.getTable(SYS_USERCONNECT);
        Variable un_col = connect_priv.getResolvedVariable(0);
        Variable proto_col = connect_priv.getResolvedVariable(1);
        Variable host_col = connect_priv.getResolvedVariable(2);
        Variable access_col = connect_priv.getResolvedVariable(3);
        // Query: where UserName = %username%
        Table t = connect_priv.simpleSelect(context, un_col, Operator.get("="),
                new Expression(TObject.stringVal(username)));
        // Query: where %protocol% like Protocol
        Expression exp = Expression.simple(TObject.stringVal(protocol),
                Operator.get("like"), proto_col);
        t = t.exhaustiveSelect(context, exp);
        // Query: where %host% like Host
        exp = Expression.simple(TObject.stringVal(host),
                Operator.get("like"), host_col);
        t = t.exhaustiveSelect(context, exp);

        // Those that are DENY
        Table t2 = t.simpleSelect(context, access_col, Operator.get("="),
                new Expression(TObject.stringVal("DENY")));
        if (t2.getRowCount() > 0) {
            return false;
        }
        // Those that are ALLOW
        Table t3 = t.simpleSelect(context, access_col, Operator.get("="),
                new Expression(TObject.stringVal("ALLOW")));
        return t3.getRowCount() > 0;
        // No DENY or ALLOW entries for this host so deny access.

    }

    /**
     * Returns true if a user exists in this database, otherwise returns
     * false.
     * <p>
     * NOTE: Assumes exclusive lock on DatabaseConnection.
     */
    public boolean userExists(DatabaseQueryContext context, String username)
            throws DatabaseException {
        DataTable table = context.getTable(SYS_PASSWORD);
        Variable c1 = table.getResolvedVariable(0);
        // All Password where UserName = %username%
        Table t = table.simpleSelect(context, c1, Operator.get("="),
                new Expression(TObject.stringVal(username)));
        return t.getRowCount() > 0;
    }

    /**
     * Creates and adds a new user to this database.  The User object for
     * the user is returned.
     * <p>
     * If the user is already defined by the database then an error is generated.
     * <p>
     * NOTE: Assumes exclusive lock on DatabaseConnection.
     */
    public void createUser(DatabaseQueryContext context,
                           String username, String password)
            throws DatabaseException {

        if (username == null || password == null) {
            throw new DatabaseException("Username or password can not be NULL.");
        }

        // The username must be more than 1 character
        if (username.length() <= 1) {
            throw new DatabaseException("Username must be at least 2 characters.");
        }

        // The password must be more than 1 character
        if (password.length() <= 1) {
            throw new DatabaseException("Password must be at least 2 characters.");
        }

        // Check the user doesn't already exist
        if (userExists(context, username)) {
            throw new DatabaseException("User '" + username + "' already exists.");
        }

        // Some usernames are reserved words
        if (username.equalsIgnoreCase("public")) {
            throw new DatabaseException("User '" + username +
                    "' not allowed - reserved.");
        }

        // Usernames starting with @, &, # and $ are reserved for system
        // identifiers
        char c = username.charAt(0);
        if (c == '@' || c == '&' || c == '#' || c == '$') {
            throw new DatabaseException("User name can not start with '" + c +
                    "' character.");
        }

        // Add this user to the password table.
        DataTable table = context.getTable(SYS_PASSWORD);
        RowData rdat = new RowData(table);
        rdat.setColumnDataFromObject(0, username);
        rdat.setColumnDataFromObject(1, password);
        table.add(rdat);

    }

    /**
     * Deletes all the groups the user belongs to.  This is intended for a user
     * alter command for setting the groups a user belongs to.
     * <p>
     * NOTE: Assumes exclusive lock on DatabaseConnection.
     */
    public void deleteAllUserGroups(DatabaseQueryContext context, String username)
            throws DatabaseException {
        Operator EQUALS_OP = Operator.get("=");
        Expression USER_EXPR = new Expression(TObject.stringVal(username));

        DataTable table = context.getTable(SYS_USERPRIV);
        Variable c1 = table.getResolvedVariable(0);
        // All UserPriv where UserName = %username%
        Table t = table.simpleSelect(context, c1, EQUALS_OP, USER_EXPR);
        // Delete all the groups
        table.delete(t);

    }

    /**
     * Deletes the user from the system.  This also deletes all information
     * associated with a user such as the groups they belong to.  It does not
     * delete the privs a user has set up.
     * <p>
     * NOTE: Assumes exclusive lock on DatabaseConnection.
     */
    public void deleteUser(DatabaseQueryContext context, String username)
            throws DatabaseException {
        // PENDING: This should check if there are any tables the user has setup
        //  and not allow the delete if there are.

        Operator EQUALS_OP = Operator.get("=");
        Expression USER_EXPR = new Expression(TObject.stringVal(username));

        // First delete all the groups from the user priv table
        deleteAllUserGroups(context, username);

        // Now delete the username from the UserConnectPriv table
        DataTable table = context.getTable(SYS_USERCONNECT);
        Variable c1 = table.getResolvedVariable(0);
        Table t = table.simpleSelect(context, c1, EQUALS_OP, USER_EXPR);
        table.delete(t);

        // Finally delete the username from the Password table
        table = context.getTable(SYS_PASSWORD);
        c1 = table.getResolvedVariable(0);
        t = table.simpleSelect(context, c1, EQUALS_OP, USER_EXPR);
        table.delete(t);

    }

    /**
     * Alters the password of the user but otherwise does not change any
     * information about the user.
     * <p>
     * NOTE: Assumes exclusive lock on DatabaseConnection.
     */
    public void alterUserPassword(DatabaseQueryContext context,
                                  String username, String password) throws DatabaseException {

        Operator EQUALS_OP = Operator.get("=");
        Expression USER_EXPR = new Expression(TObject.stringVal(username));

        // Delete the current username from the Password table
        DataTable table = context.getTable(SYS_PASSWORD);
        Variable c1 = table.getResolvedVariable(0);
        Table t = table.simpleSelect(context, c1, EQUALS_OP, USER_EXPR);
        if (t.getRowCount() == 1) {
            table.delete(t);

            // Add the new username
            table = context.getTable(SYS_PASSWORD);
            RowData rdat = new RowData(table);
            rdat.setColumnDataFromObject(0, username);
            rdat.setColumnDataFromObject(1, password);
            table.add(rdat);

        } else {
            throw new DatabaseException("Username '" + username + "' was not found.");
        }

    }

    /**
     * Returns the list of all user groups the user belongs to.
     */
    public String[] groupsUserBelongsTo(DatabaseQueryContext context,
                                        String username) throws DatabaseException {

        DataTable table = context.getTable(SYS_USERPRIV);
        Variable c1 = table.getResolvedVariable(0);
        // All UserPriv where UserName = %username%
        Table t = table.simpleSelect(context, c1, Operator.get("="),
                new Expression(TObject.stringVal(username)));
        int sz = t.getRowCount();
        String[] groups = new String[sz];
        RowEnumeration row_enum = t.rowEnumeration();
        int i = 0;
        while (row_enum.hasMoreRows()) {
            groups[i] = t.getCellContents(1,
                    row_enum.nextRowIndex()).getObject().toString();
            ++i;
        }

        return groups;
    }

    /**
     * Returns true if the given user belongs to the given group otherwise
     * returns false.
     * <p>
     * NOTE: Assumes exclusive lock on DatabaseConnection.
     */
    public boolean userBelongsToGroup(DatabaseQueryContext context,
                                      String username, String group)
            throws DatabaseException {

        DataTable table = context.getTable(SYS_USERPRIV);
        Variable c1 = table.getResolvedVariable(0);
        Variable c2 = table.getResolvedVariable(1);
        // All UserPriv where UserName = %username%
        Table t = table.simpleSelect(context, c1, Operator.get("="),
                new Expression(TObject.stringVal(username)));
        // All from this set where PrivGroupName = %group%
        t = t.simpleSelect(context, c2, Operator.get("="),
                new Expression(TObject.stringVal(group)));
        return t.getRowCount() > 0;
    }

    /**
     * Adds the user to the given group.  This makes an entry in the UserPriv
     * for this user and the given group.  If the user already belongs to the
     * group then no changes are made.
     * <p>
     * It is important that any security checks for ensuring the grantee is
     * allowed to give the user these privs are preformed before this method is
     * called.
     * <p>
     * NOTE: Assumes exclusive lock on DatabaseConnection.
     */
    public void addUserToGroup(DatabaseQueryContext context,
                               String username, String group)
            throws DatabaseException {
        if (group == null) {
            throw new DatabaseException("Can add NULL group.");
        }

        // Groups starting with @, &, # and $ are reserved for system
        // identifiers
        char c = group.charAt(0);
        if (c == '@' || c == '&' || c == '#' || c == '$') {
            throw new DatabaseException("The group name can not start with '" + c +
                    "' character.");
        }

        // Check the user doesn't belong to the group
        if (!userBelongsToGroup(context, username, group)) {
            // The user priv table
            DataTable table = context.getTable(SYS_USERPRIV);
            // Add this user to the group.
            RowData rdat = new RowData(table);
            rdat.setColumnDataFromObject(0, username);
            rdat.setColumnDataFromObject(1, group);
            table.add(rdat);
        }
        // NOTE: we silently ignore the case when a user already belongs to the
        //   group.
    }

    /**
     * Sets the lock status for the given user.  If a user account if locked, it
     * is rejected from logging in to the database.
     * <p>
     * It is important that any security checks to determine if the process
     * setting the user lock is allowed to do it is done before this method is
     * called.
     * <p>
     * NOTE: Assumes exclusive lock on DatabaseConnection.
     */
    public void setUserLock(DatabaseQueryContext context, User user,
                            boolean lock_status) throws DatabaseException {

        String username = user.getUserName();

        // Internally we implement this by adding the user to the #locked group.
        DataTable table = context.getTable(SYS_USERPRIV);
        Variable c1 = table.getResolvedVariable(0);
        Variable c2 = table.getResolvedVariable(1);
        // All UserPriv where UserName = %username%
        Table t = table.simpleSelect(context, c1, Operator.get("="),
                new Expression(TObject.stringVal(username)));
        // All from this set where PrivGroupName = %group%
        t = t.simpleSelect(context, c2, Operator.get("="),
                new Expression(TObject.stringVal(LOCK_GROUP)));

        boolean user_belongs_to_lock_group = t.getRowCount() > 0;
        if (lock_status && !user_belongs_to_lock_group) {
            // Lock the user by adding the user to the lock group
            // Add this user to the locked group.
            RowData rdat = new RowData(table);
            rdat.setColumnDataFromObject(0, username);
            rdat.setColumnDataFromObject(1, LOCK_GROUP);
            table.add(rdat);
        } else if (!lock_status && user_belongs_to_lock_group) {
            // Unlock the user by removing the user from the lock group
            // Remove this user from the locked group.
            table.delete(t);
        }

    }

    /**
     * Grants the given user access to connect to the database from the
     * given host address.  The 'protocol' string is the connecting protocol
     * which can be either 'TCP' or 'Local'.  The 'host' string is the actual
     * host that is connecting.  For example, if the protocol was TCP then
     * the client host may be '127.0.0.1' for localhost.
     */
    public void grantHostAccessToUser(DatabaseQueryContext context,
                                      String user, String protocol, String host)
            throws DatabaseException {

        // The user connect priv table
        DataTable table = context.getTable(SYS_USERCONNECT);
        // Add the protocol and host to the table
        RowData rdat = new RowData(table);
        rdat.setColumnDataFromObject(0, user);
        rdat.setColumnDataFromObject(1, protocol);
        rdat.setColumnDataFromObject(2, host);
        rdat.setColumnDataFromObject(3, "ALLOW");
        table.add(rdat);

    }

    /**
     * Returns true if the user belongs to the secure access priv group.
     */
    private boolean userHasSecureAccess(DatabaseQueryContext context, User user)
            throws DatabaseException {
        // The internal secure user has full privs on everything
        if (user.getUserName().equals(INTERNAL_SECURE_USERNAME)) {
            return true;
        }
        return userBelongsToGroup(context, user.getUserName(), SECURE_GROUP);
    }

    /**
     * Returns true if the grant manager permits a schema operation (eg,
     * CREATE, ALTER and DROP table operations) for the given user.
     */
    private boolean userHasSchemaGrant(DatabaseQueryContext context,
                                       User user, String schema, int grant) throws DatabaseException {
        // The internal secure user has full privs on everything
        if (user.getUserName().equals(INTERNAL_SECURE_USERNAME)) {
            return true;
        }

        // No users have schema access to the system schema.
        if (schema.equals(SYSTEM_SCHEMA)) {
            return false;
        }

        // Ask the grant manager if there are any privs setup for this user on the
        // given schema.
        GrantManager manager = context.getGrantManager();
        Privileges privs = manager.userGrants(
                GrantManager.SCHEMA, schema, user.getUserName());

        return privs.permits(grant);
    }

    /**
     * Returns true if the grant manager permits a table object operation (eg,
     * SELECT, INSERT, UPDATE, DELETE and COMPACT table operations) for the given
     * user.
     */
    private boolean userHasTableObjectGrant(DatabaseQueryContext context,
                                            User user, TableName table_name, Variable[] columns,
                                            int grant) throws DatabaseException {

        // The internal secure user has full privs on everything
        if (user.getUserName().equals(INTERNAL_SECURE_USERNAME)) {
            return true;
        }

        // PENDING: Support column level privileges.

        // Ask the grant manager if there are any privs setup for this user on the
        // given schema.
        GrantManager manager = context.getGrantManager();
        Privileges privs = manager.userGrants(
                GrantManager.TABLE, table_name.toString(), user.getUserName());

        return privs.permits(grant);
    }

    /**
     * Returns true if the user is permitted to create, alter and drop user
     * information from the database, otherwise returns false.  Only members of
     * the 'secure access' group, or the 'user manager' group can do this.
     */
    public boolean canUserCreateAndDropUsers(
            DatabaseQueryContext context, User user) throws DatabaseException {
        return (userHasSecureAccess(context, user) ||
                userBelongsToGroup(context, user.getUserName(),
                        USER_MANAGER_GROUP));
    }

    /**
     * Returns true if the user is permitted to create and drop schema's in the
     * database, otherwise returns false.  Only members of the 'secure access'
     * group, or the 'schema manager' group can do this.
     */
    public boolean canUserCreateAndDropSchema(
            DatabaseQueryContext context, User user, String schema)
            throws DatabaseException {

        // The internal secure user has full privs on everything
        if (user.getUserName().equals(INTERNAL_SECURE_USERNAME)) {
            return true;
        }

        // No user can create or drop the system schema.
        if (schema.equals(SYSTEM_SCHEMA)) {
            return false;
        } else {
            return (userHasSecureAccess(context, user) ||
                    userBelongsToGroup(context, user.getUserName(),
                            SCHEMA_MANAGER_GROUP));
        }
    }

    /**
     * Returns true if the user can shut down the database server.  A user can
     * shut down the database if they are a member of the 'secure acces' group.
     */
    public boolean canUserShutDown(DatabaseQueryContext context, User user)
            throws DatabaseException {
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user is allowed to execute the given stored procedure.
     */
    public boolean canUserExecuteStoredProcedure(DatabaseQueryContext context,
                                                 User user, String procedure_name) throws DatabaseException {
        // Currently you can only execute a procedure if you are a member of the
        // secure access priv group.
        return userHasSecureAccess(context, user);
    }

    // ---- General schema level privs ----

    /**
     * Returns true if the user can create a table or view with the given name,
     * otherwise returns false.
     */
    public boolean canUserCreateTableObject(
            DatabaseQueryContext context, User user, TableName table)
            throws DatabaseException {
        if (userHasSchemaGrant(context, user,
                table.getSchema(), Privileges.CREATE)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can alter a table or view with the given name,
     * otherwise returns false.
     */
    public boolean canUserAlterTableObject(
            DatabaseQueryContext context, User user, TableName table)
            throws DatabaseException {
        if (userHasSchemaGrant(context, user,
                table.getSchema(), Privileges.ALTER)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can drop a table or view with the given name,
     * otherwise returns false.
     */
    public boolean canUserDropTableObject(
            DatabaseQueryContext context, User user, TableName table)
            throws DatabaseException {
        if (userHasSchemaGrant(context, user,
                table.getSchema(), Privileges.DROP)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    // ---- Check table object privs ----

    /**
     * Returns true if the user can select from a table or view with the given
     * name and given columns, otherwise returns false.
     */
    public boolean canUserSelectFromTableObject(
            DatabaseQueryContext context, User user, TableName table,
            Variable[] columns) throws DatabaseException {
        if (userHasTableObjectGrant(context, user, table, columns,
                Privileges.SELECT)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can insert into a table or view with the given
     * name and given columns, otherwise returns false.
     */
    public boolean canUserInsertIntoTableObject(
            DatabaseQueryContext context, User user, TableName table,
            Variable[] columns) throws DatabaseException {
        if (userHasTableObjectGrant(context, user, table, columns,
                Privileges.INSERT)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can update a table or view with the given
     * name and given columns, otherwise returns false.
     */
    public boolean canUserUpdateTableObject(
            DatabaseQueryContext context, User user, TableName table,
            Variable[] columns) throws DatabaseException {
        if (userHasTableObjectGrant(context, user, table, columns,
                Privileges.UPDATE)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can delete from a table or view with the given
     * name and given columns, otherwise returns false.
     */
    public boolean canUserDeleteFromTableObject(
            DatabaseQueryContext context, User user, TableName table)
            throws DatabaseException {
        if (userHasTableObjectGrant(context, user, table, null,
                Privileges.DELETE)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can compact a table with the given name,
     * otherwise returns false.
     */
    public boolean canUserCompactTableObject(
            DatabaseQueryContext context, User user, TableName table)
            throws DatabaseException {
        if (userHasTableObjectGrant(context, user, table, null,
                Privileges.COMPACT)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can create a procedure with the given name,
     * otherwise returns false.
     */
    public boolean canUserCreateProcedureObject(
            DatabaseQueryContext context, User user, TableName table)
            throws DatabaseException {
        if (userHasSchemaGrant(context, user,
                table.getSchema(), Privileges.CREATE)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can drop a procedure with the given name,
     * otherwise returns false.
     */
    public boolean canUserDropProcedureObject(
            DatabaseQueryContext context, User user, TableName table)
            throws DatabaseException {
        if (userHasSchemaGrant(context, user,
                table.getSchema(), Privileges.DROP)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can create a sequence with the given name,
     * otherwise returns false.
     */
    public boolean canUserCreateSequenceObject(
            DatabaseQueryContext context, User user, TableName table)
            throws DatabaseException {
        if (userHasSchemaGrant(context, user,
                table.getSchema(), Privileges.CREATE)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }

    /**
     * Returns true if the user can drop a sequence with the given name,
     * otherwise returns false.
     */
    public boolean canUserDropSequenceObject(
            DatabaseQueryContext context, User user, TableName table)
            throws DatabaseException {
        if (userHasSchemaGrant(context, user,
                table.getSchema(), Privileges.DROP)) {
            return true;
        }

        // If the user belongs to the secure access priv group, return true
        return userHasSecureAccess(context, user);
    }


    // ---------- Schema management ----------

    /**
     * Creates the schema information tables introducted in version 0.90.  The
     * schema information tables are;
     */
    void createSchemaInfoTables(DatabaseConnection connection)
            throws DatabaseException {

        connection.createSchema(DEFAULT_SCHEMA, "DEFAULT");
        connection.createSchema(JDBC_SCHEMA, "SYSTEM");

    }

    /**
     * Creates all the system views.
     */
    private void createSystemViews(DatabaseConnection connection)
            throws DatabaseException {
        // Obtain the JDBC interface.
        try {
            Connection jdbc = connection.getJDBCConnection();

            // Is the username/password in the database?
            Statement stmt = jdbc.createStatement();

            // This view shows the grants that the user has (no join, only priv_bit).
            stmt.executeUpdate(
                    "CREATE VIEW SYS_JDBC.ThisUserSimpleGrant AS " +
                            "  SELECT \"priv_bit\", \"object\", \"param\", \"grantee\", " +
                            "         \"grant_option\", \"granter\" " +
                            "    FROM SYS_INFO.Grant " +
                            "   WHERE ( grantee = user() OR grantee = '@PUBLIC' )");
            // This view shows the grants that the user is allowed to see
            stmt.executeUpdate(
                    "CREATE VIEW SYS_JDBC.ThisUserGrant AS " +
                            "  SELECT \"description\", \"object\", \"param\", \"grantee\", " +
                            "         \"grant_option\", \"granter\" " +
                            "    FROM SYS_INFO.Grant, SYS_INFO.PrivMap " +
                            "   WHERE ( grantee = user() OR grantee = '@PUBLIC' )" +
                            "     AND Grant.priv_bit = PrivMap.priv_bit");
            // A view that represents the list of schema this user is allowed to view
            // the contents of.
            stmt.executeUpdate(
                    "CREATE VIEW SYS_JDBC.ThisUserSchemaInfo AS " +
                            "  SELECT * FROM SYS_INFO.SchemaInfo " +
                            "   WHERE \"name\" IN ( " +
                            "     SELECT \"param\" " +
                            "       FROM SYS_JDBC.ThisUserGrant " +
                            "      WHERE \"object\" = 65 " +
                            "        AND \"description\" = 'LIST' )");
            // A view that exposes the TableColumn table but only for the tables
            // this user has read access to.
            stmt.executeUpdate(
                    "CREATE VIEW SYS_JDBC.ThisUserTableColumns AS " +
                            "  SELECT * FROM SYS_INFO.TableColumns " +
                            "   WHERE \"schema\" IN ( " +
                            "     SELECT \"name\" FROM SYS_JDBC.ThisUserSchemaInfo )");
            // A view that exposes the TableInfo table but only for the tables
            // this user has read access to.
            stmt.executeUpdate(
                    "CREATE VIEW SYS_JDBC.ThisUserTableInfo AS " +
                            "  SELECT * FROM SYS_INFO.TableInfo " +
                            "   WHERE \"schema\" IN ( " +
                            "     SELECT \"name\" FROM SYS_JDBC.ThisUserSchemaInfo )");

            // A JDBC helper view for the 'getTables' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.Tables AS " +
                            "  SELECT NULL AS \"TABLE_CAT\", \n" +
                            "         \"schema\" AS \"TABLE_SCHEM\", \n" +
                            "         \"name\" AS \"TABLE_NAME\", \n" +
                            "         \"type\" AS \"TABLE_TYPE\", \n" +
                            "         \"other\" AS \"REMARKS\", \n" +
                            "         NULL AS \"TYPE_CAT\", \n" +
                            "         NULL AS \"TYPE_SCHEM\", \n" +
                            "         NULL AS \"TYPE_NAME\", \n" +
                            "         NULL AS \"SELF_REFERENCING_COL_NAME\", \n" +
                            "         NULL AS \"REF_GENERATION\" \n" +
                            "    FROM SYS_JDBC.ThisUserTableInfo \n");
            // A JDBC helper view for the 'getSchemas' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.Schemas AS " +
                            "  SELECT \"name\" AS \"TABLE_SCHEM\", \n" +
                            "         NULL AS \"TABLE_CATALOG\" \n" +
                            "    FROM SYS_JDBC.ThisUserSchemaInfo\n");
            // A JDBC helper view for the 'getCatalogs' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.Catalogs AS " +
                            "  SELECT NULL AS \"TABLE_CAT\" \n" +
                            "    FROM SYS_INFO.SchemaInfo\n" + // Hacky, this will generate a 0 row
                            "   WHERE FALSE\n");                   // table.
            // A JDBC helper view for the 'getColumns' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.Columns AS " +
                            "  SELECT NULL AS \"TABLE_CAT\",\n" +
                            "         \"schema\" AS \"TABLE_SCHEM\",\n" +
                            "         \"table\" AS \"TABLE_NAME\",\n" +
                            "         \"column\" AS \"COLUMN_NAME\",\n" +
                            "         \"sql_type\" AS \"DATA_TYPE\",\n" +
                            "         \"type_desc\" AS \"TYPE_NAME\",\n" +
                            "         IF(\"size\" = -1, 1024, \"size\") AS \"COLUMN_SIZE\",\n" +
                            "         NULL AS \"BUFFER_LENGTH\",\n" +
                            "         \"scale\" AS \"DECIMAL_DIGITS\",\n" +
                            "         IF(\"sql_type\" = -7, 2, 10) AS \"NUM_PREC_RADIX\",\n" +
                            "         IF(\"not_null\", 0, 1) AS \"NULLABLE\",\n" +
                            "         '' AS \"REMARKS\",\n" +
                            "         \"default\" AS \"COLUMN_DEF\",\n" +
                            "         NULL AS \"SQL_DATA_TYPE\",\n" +
                            "         NULL AS \"SQL_DATETIME_SUB\",\n" +
                            "         IF(\"size\" = -1, 1024, \"size\") AS \"CHAR_OCTET_LENGTH\",\n" +
                            "         \"seq_no\" + 1 AS \"ORDINAL_POSITION\",\n" +
                            "         IF(\"not_null\", 'NO', 'YES') AS \"IS_NULLABLE\"\n" +
                            "    FROM SYS_JDBC.ThisUserTableColumns\n");
            // A JDBC helper view for the 'getColumnPrivileges' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.ColumnPrivileges AS " +
                            "  SELECT \"TABLE_CAT\",\n" +
                            "         \"TABLE_SCHEM\",\n" +
                            "         \"TABLE_NAME\",\n" +
                            "         \"COLUMN_NAME\",\n" +
                            "         IF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
                            "                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
                            "         IF(\"ThisUserGrant.grantee\" = '@PUBLIC', \n" +
                            "                    'public', \"ThisUserGrant.grantee\") AS \"GRANTEE\",\n" +
                            "         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
                            "         IF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
                            "    FROM SYS_JDBC.Columns, SYS_JDBC.ThisUserGrant \n" +
                            "   WHERE CONCAT(Columns.TABLE_SCHEM, '.', Columns.TABLE_NAME) = \n" +
                            "         ThisUserGrant.param \n" +
                            "     AND SYS_JDBC.ThisUserGrant.object = 1 \n" +
                            "     AND SYS_JDBC.ThisUserGrant.description IS NOT NULL \n");
            // A JDBC helper view for the 'getTablePrivileges' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.TablePrivileges AS " +
                            "  SELECT \"TABLE_CAT\",\n" +
                            "         \"TABLE_SCHEM\",\n" +
                            "         \"TABLE_NAME\",\n" +
                            "         IF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
                            "                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
                            "         IF(\"ThisUserGrant.grantee\" = '@PUBLIC', \n" +
                            "                    'public', \"ThisUserGrant.grantee\") AS \"GRANTEE\",\n" +
                            "         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
                            "         IF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
                            "    FROM SYS_JDBC.Tables, SYS_JDBC.ThisUserGrant \n" +
                            "   WHERE CONCAT(Tables.TABLE_SCHEM, '.', Tables.TABLE_NAME) = \n" +
                            "         ThisUserGrant.param \n" +
                            "     AND SYS_JDBC.ThisUserGrant.object = 1 \n" +
                            "     AND SYS_JDBC.ThisUserGrant.description IS NOT NULL \n");
            // A JDBC helper view for the 'getPrimaryKeys' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.PrimaryKeys AS " +
                            "  SELECT NULL \"TABLE_CAT\",\n" +
                            "         \"schema\" \"TABLE_SCHEM\",\n" +
                            "         \"table\" \"TABLE_NAME\",\n" +
                            "         \"column\" \"COLUMN_NAME\",\n" +
                            "         \"SYS_INFO.PrimaryColumns.seq_no\" \"KEY_SEQ\",\n" +
                            "         \"name\" \"PK_NAME\"\n" +
                            "    FROM SYS_INFO.PKeyInfo, SYS_INFO.PrimaryColumns\n" +
                            "   WHERE PKeyInfo.id = PrimaryColumns.pk_id\n" +
                            "     AND \"schema\" IN\n" +
                            "            ( SELECT \"name\" FROM SYS_JDBC.ThisUserSchemaInfo )\n");
            // A JDBC helper view for the 'getImportedKeys' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.ImportedKeys AS " +
                            "  SELECT NULL \"PKTABLE_CAT\",\n" +
                            "         \"FKeyInfo.ref_schema\" \"PKTABLE_SCHEM\",\n" +
                            "         \"FKeyInfo.ref_table\" \"PKTABLE_NAME\",\n" +
                            "         \"ForeignColumns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
                            "         NULL \"FKTABLE_CAT\",\n" +
                            "         \"FKeyInfo.schema\" \"FKTABLE_SCHEM\",\n" +
                            "         \"FKeyInfo.table\" \"FKTABLE_NAME\",\n" +
                            "         \"ForeignColumns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
                            "         \"ForeignColumns.seq_no\" \"KEY_SEQ\",\n" +
                            "         I_FRULE_CONVERT(\"FKeyInfo.update_rule\") \"UPDATE_RULE\",\n" +
                            "         I_FRULE_CONVERT(\"FKeyInfo.delete_rule\") \"DELETE_RULE\",\n" +
                            "         \"FKeyInfo.name\" \"FK_NAME\",\n" +
                            "         NULL \"PK_NAME\",\n" +
                            "         \"FKeyInfo.deferred\" \"DEFERRABILITY\"\n" +
                            "    FROM SYS_INFO.FKeyInfo, SYS_INFO.ForeignColumns\n" +
                            "   WHERE FKeyInfo.id = ForeignColumns.fk_id\n" +
                            "     AND \"FKeyInfo.schema\" IN\n" +
                            "              ( SELECT \"name\" FROM SYS_JDBC.ThisUserSchemaInfo )\n");
            // A JDBC helper view for the 'getExportedKeys' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.ExportedKeys AS " +
                            "  SELECT NULL \"PKTABLE_CAT\",\n" +
                            "         \"FKeyInfo.ref_schema\" \"PKTABLE_SCHEM\",\n" +
                            "         \"FKeyInfo.ref_table\" \"PKTABLE_NAME\",\n" +
                            "         \"ForeignColumns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
                            "         NULL \"FKTABLE_CAT\",\n" +
                            "         \"FKeyInfo.schema\" \"FKTABLE_SCHEM\",\n" +
                            "         \"FKeyInfo.table\" \"FKTABLE_NAME\",\n" +
                            "         \"ForeignColumns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
                            "         \"ForeignColumns.seq_no\" \"KEY_SEQ\",\n" +
                            "         I_FRULE_CONVERT(\"FKeyInfo.update_rule\") \"UPDATE_RULE\",\n" +
                            "         I_FRULE_CONVERT(\"FKeyInfo.delete_rule\") \"DELETE_RULE\",\n" +
                            "         \"FKeyInfo.name\" \"FK_NAME\",\n" +
                            "         NULL \"PK_NAME\",\n" +
                            "         \"FKeyInfo.deferred\" \"DEFERRABILITY\"\n" +
                            "    FROM SYS_INFO.FKeyInfo, SYS_INFO.ForeignColumns\n" +
                            "   WHERE FKeyInfo.id = ForeignColumns.fk_id\n" +
                            "     AND \"FKeyInfo.schema\" IN\n" +
                            "              ( SELECT \"name\" FROM SYS_JDBC.ThisUserSchemaInfo )\n");
            // A JDBC helper view for the 'getCrossReference' meta-data method
            stmt.executeUpdate(
                    "  CREATE VIEW SYS_JDBC.CrossReference AS " +
                            "  SELECT NULL \"PKTABLE_CAT\",\n" +
                            "         \"FKeyInfo.ref_schema\" \"PKTABLE_SCHEM\",\n" +
                            "         \"FKeyInfo.ref_table\" \"PKTABLE_NAME\",\n" +
                            "         \"ForeignColumns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
                            "         NULL \"FKTABLE_CAT\",\n" +
                            "         \"FKeyInfo.schema\" \"FKTABLE_SCHEM\",\n" +
                            "         \"FKeyInfo.table\" \"FKTABLE_NAME\",\n" +
                            "         \"ForeignColumns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
                            "         \"ForeignColumns.seq_no\" \"KEY_SEQ\",\n" +
                            "         I_FRULE_CONVERT(\"FKeyInfo.update_rule\") \"UPDATE_RULE\",\n" +
                            "         I_FRULE_CONVERT(\"FKeyInfo.delete_rule\") \"DELETE_RULE\",\n" +
                            "         \"FKeyInfo.name\" \"FK_NAME\",\n" +
                            "         NULL \"PK_NAME\",\n" +
                            "         \"FKeyInfo.deferred\" \"DEFERRABILITY\"\n" +
                            "    FROM SYS_INFO.FKeyInfo, SYS_INFO.ForeignColumns\n" +
                            "   WHERE FKeyInfo.id = ForeignColumns.fk_id\n" +
                            "     AND \"FKeyInfo.schema\" IN\n" +
                            "              ( SELECT \"name\" FROM SYS_JDBC.ThisUserSchemaInfo )\n");

        } catch (SQLException e) {
            if (e instanceof MSQLException) {
                MSQLException msqle = (MSQLException) e;
                Debug().write(Lvl.ERROR, this,
                        msqle.getServerErrorStackTrace());
            }
            Debug().writeException(Lvl.ERROR, e);
            throw new RuntimeException("SQL Error: " + e.getMessage());
        }

    }

    /**
     * Creates all the priv/password system tables.
     */
    private void createSystemTables(DatabaseConnection connection)
            throws DatabaseException {

        // --- The user management tables ---
        DataTableDef Password = new DataTableDef();
        Password.setTableName(SYS_PASSWORD);
        Password.addColumn(DataTableColumnDef.createStringColumn("UserName"));
        Password.addColumn(DataTableColumnDef.createStringColumn("Password"));

        DataTableDef UserPriv = new DataTableDef();
        UserPriv.setTableName(SYS_USERPRIV);
        UserPriv.addColumn(DataTableColumnDef.createStringColumn("UserName"));
        UserPriv.addColumn(
                DataTableColumnDef.createStringColumn("PrivGroupName"));

        DataTableDef UserConnectPriv = new DataTableDef();
        UserConnectPriv.setTableName(SYS_USERCONNECT);
        UserConnectPriv.addColumn(
                DataTableColumnDef.createStringColumn("UserName"));
        UserConnectPriv.addColumn(
                DataTableColumnDef.createStringColumn("Protocol"));
        UserConnectPriv.addColumn(
                DataTableColumnDef.createStringColumn("Host"));
        UserConnectPriv.addColumn(
                DataTableColumnDef.createStringColumn("Access"));

        DataTableDef Grant = new DataTableDef();
        Grant.setTableName(SYS_GRANTS);
        Grant.addColumn(DataTableColumnDef.createNumericColumn("priv_bit"));
        Grant.addColumn(DataTableColumnDef.createNumericColumn("object"));
        Grant.addColumn(DataTableColumnDef.createStringColumn("param"));
        Grant.addColumn(DataTableColumnDef.createStringColumn("grantee"));
        Grant.addColumn(DataTableColumnDef.createStringColumn("grant_option"));
        Grant.addColumn(DataTableColumnDef.createStringColumn("granter"));

        DataTableDef Service = new DataTableDef();
        Service.setTableName(SYS_SERVICE);
        Service.addColumn(DataTableColumnDef.createStringColumn("name"));
        Service.addColumn(DataTableColumnDef.createStringColumn("class"));
        Service.addColumn(DataTableColumnDef.createStringColumn("type"));

        DataTableDef FunctionFactory = new DataTableDef();
        FunctionFactory.setTableName(SYS_FUNCTIONFACTORY);
        FunctionFactory.addColumn(
                DataTableColumnDef.createStringColumn("name"));
        FunctionFactory.addColumn(
                DataTableColumnDef.createStringColumn("class"));
        FunctionFactory.addColumn(
                DataTableColumnDef.createStringColumn("type"));

        DataTableDef Function = new DataTableDef();
        Function.setTableName(SYS_FUNCTION);
        Function.addColumn(DataTableColumnDef.createStringColumn("schema"));
        Function.addColumn(DataTableColumnDef.createStringColumn("name"));
        Function.addColumn(DataTableColumnDef.createStringColumn("type"));
        Function.addColumn(DataTableColumnDef.createStringColumn("location"));
        Function.addColumn(DataTableColumnDef.createStringColumn("return_type"));
        Function.addColumn(DataTableColumnDef.createStringColumn("args_type"));
        Function.addColumn(DataTableColumnDef.createStringColumn("username"));

        DataTableDef View = new DataTableDef();
        View.setTableName(SYS_VIEW);
        View.addColumn(DataTableColumnDef.createStringColumn("schema"));
        View.addColumn(DataTableColumnDef.createStringColumn("name"));
        View.addColumn(DataTableColumnDef.createBinaryColumn("query"));
        View.addColumn(DataTableColumnDef.createBinaryColumn("data"));
        View.addColumn(DataTableColumnDef.createStringColumn("username"));

        DataTableDef Label = new DataTableDef();
        Label.setTableName(SYS_LABEL);
        Label.addColumn(DataTableColumnDef.createNumericColumn("object_type"));
        Label.addColumn(DataTableColumnDef.createStringColumn("object_name"));
        Label.addColumn(DataTableColumnDef.createStringColumn("label"));

        DataTableDef DataTrigger = new DataTableDef();
        DataTrigger.setTableName(SYS_DATA_TRIGGER);
        DataTrigger.addColumn(DataTableColumnDef.createStringColumn("schema"));
        DataTrigger.addColumn(DataTableColumnDef.createStringColumn("name"));
        DataTrigger.addColumn(DataTableColumnDef.createNumericColumn("type"));
        DataTrigger.addColumn(DataTableColumnDef.createStringColumn("on_object"));
        DataTrigger.addColumn(DataTableColumnDef.createStringColumn("action"));
        DataTrigger.addColumn(DataTableColumnDef.createBinaryColumn("misc"));
        DataTrigger.addColumn(DataTableColumnDef.createStringColumn("username"));

        // Create the tables
        connection.alterCreateTable(Password, 91, 128);
        connection.alterCreateTable(UserPriv, 91, 128);
        connection.alterCreateTable(UserConnectPriv, 91, 128);
        connection.alterCreateTable(Grant, 195, 128);
        connection.alterCreateTable(Service, 91, 128);
        connection.alterCreateTable(FunctionFactory, 91, 128);
        connection.alterCreateTable(Function, 91, 128);
        connection.alterCreateTable(View, 91, 128);
        connection.alterCreateTable(Label, 91, 128);
        connection.alterCreateTable(DataTrigger, 91, 128);

    }

    /**
     * Sets all the standard functions and procedures available to engine.
     * This creates an entry in the SYS_FUNCTION table for all the dynamic
     * functions and procedures.  This may not include the functions exposed
     * though the FunctionFactory interface.
     */
    public void setupSystemFunctions(DatabaseConnection connection,
                                     String admin_user) throws DatabaseException {

        final String GRANTER = INTERNAL_SECURE_USERNAME;

        // The manager handling the functions.
        ProcedureManager manager = connection.getProcedureManager();

        // Define the SYSTEM_MAKE_BACKUP procedure
        Class c = com.pony.database.procedure.SystemBackup.class;
        manager.defineJavaProcedure(
                new ProcedureName(SYSTEM_SCHEMA, "SYSTEM_MAKE_BACKUP"),
                "com.pony.database.procedure.SystemBackup.invoke(ProcedureConnection, String)",
                TType.STRING_TYPE, new TType[]{TType.STRING_TYPE},
                admin_user);

        // -----

        // Set the grants for the procedures.
        GrantManager grants = connection.getGrantManager();

        // Revoke all existing grants on the internal stored procedures.
        grants.revokeAllGrantsOnObject(GrantManager.TABLE,
                "SYS_INFO.SYSTEM_MAKE_BACKUP");

        // Grant execute priv with grant option to administrator
        grants.addGrant(Privileges.PROCEDURE_EXECUTE_PRIVS,
                GrantManager.TABLE,
                "SYS_INFO.SYSTEM_MAKE_BACKUP",
                admin_user, true, GRANTER);

    }

    /**
     * Clears all the grant information in the Grant table.  This should only
     * be used if we need to refresh the grant information for whatever reason
     * (such as when converting between different versions).
     */
    private void clearAllGrants(DatabaseConnection connection)
            throws DatabaseException {
        DataTable grant_table = connection.getTable(SYS_GRANTS);
        grant_table.delete(grant_table);
    }

    /**
     * Set up the system table grants.
     * <p>
     * This gives the grantee user full access to Password,
     * UserPriv, UserConnectPriv, Service, FunctionFactory,
     * and Function.  All other  tables are granted SELECT only.
     * If 'grant_option' is true then the user is given the option to give the
     * grants to other users.
     */
    private void setSystemGrants(DatabaseConnection connection,
                                 String grantee) throws DatabaseException {

        final String GRANTER = INTERNAL_SECURE_USERNAME;

        // Add all priv grants to those that the system user is allowed to change
        GrantManager manager = connection.getGrantManager();

        // Add schema grant for APP
        manager.addGrant(Privileges.SCHEMA_ALL_PRIVS, GrantManager.SCHEMA,
                "APP",
                grantee, true, GRANTER);
        // Add public grant for SYS_INFO
        manager.addGrant(Privileges.SCHEMA_READ_PRIVS, GrantManager.SCHEMA,
                "SYS_INFO",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        // Add public grant for SYS_JDBC
        manager.addGrant(Privileges.SCHEMA_READ_PRIVS, GrantManager.SCHEMA,
                "SYS_JDBC",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);

        // For all tables in the SYS_INFO schema, grant all privileges to the
        // system user.
        manager.addGrantToAllTablesInSchema("SYS_INFO",
                Privileges.TABLE_ALL_PRIVS, grantee, false, GRANTER);

        // Set the public grants for the system tables,
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_INFO.ConnectionInfo",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_INFO.CurrentConnections",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_INFO.DatabaseStatistics",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_INFO.DatabaseVars",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_INFO.ProductInfo",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_INFO.SQLTypeInfo",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);

        // Set public grants for the system views.
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.ThisUserGrant",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.ThisUserSimpleGrant",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.ThisUserSchemaInfo",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.ThisUserTableColumns",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.ThisUserTableInfo",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);

        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.Tables",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.Schemas",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.Catalogs",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.Columns",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.ColumnPrivileges",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.TablePrivileges",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.PrimaryKeys",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.ImportedKeys",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.ExportedKeys",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);
        manager.addGrant(Privileges.TABLE_READ_PRIVS, GrantManager.TABLE,
                "SYS_JDBC.CrossReference",
                GrantManager.PUBLIC_USERNAME_STR, false, GRANTER);

    }

    /**
     * Sets the system table listeners on the SYS_INFO.View table.  These
     * listeners are used to cache information
     * that is stored and retrieved from those tables.
     */
    private void setSystemTableListeners() {
//    getSystem().addMasterTableListener(SYS_VIEW, new ViewTableListener());
    }

    /**
     * Goes through all tables in the database not in the SYS_INFO schema and
     * adds an entry in the grant table for it.
     * <p>
     * This is for converting from a pre-grant database.
     *
     * @param connection the database transaction
     * @param grantee the grantee to apply the table privs to
     */
    private void convertPreGrant(DatabaseConnection connection,
                                 String grantee) throws DatabaseException {

        String GRANTER = INTERNAL_SECURE_USERNAME;
        GrantManager manager = connection.getGrantManager();

        // Setup grants for any user schema that have been created.
        SchemaDef[] all_schema = connection.getSchemaList();
        for (int i = 0; i < all_schema.length; ++i) {
            SchemaDef schema = all_schema[i];
            // The admin user is given full privs to all tables in USER or DEFAULT
            // schema.
            if (schema.getType().equals("USER") ||
                    schema.getType().equals("DEFAULT")) {
                // Don't set grants for default schema
                if (!schema.getType().equals("DEFAULT")) {
                    manager.addGrant(Privileges.TABLE_ALL_PRIVS, GrantManager.SCHEMA,
                            schema.getName(), grantee, true, GRANTER);
                }
                manager.addGrantToAllTablesInSchema(schema.getName(),
                        Privileges.TABLE_ALL_PRIVS, grantee, true, GRANTER);
            }
        }

    }

    /**
     * Converts tables from a database that are pre database schema.
     */
    private void convertPreSchema(DatabaseConnection connection)
            throws DatabaseException {
        throw new DatabaseException(
                "Converting from pre-schema no longer supported.");
    }

    /**
     * Creates and sets up a new database to an initial empty state.  The
     * creation process involves creating all the system tables and views, adding
     * an administrator user account, creating schema, and setting up the initial
     * grant information for the administrator user.
     * <p>
     * The 'username' and 'password' parameter given are set for the administrator
     * account.
     */
    public void create(String username, String password) {

        if (isReadOnly()) {
            throw new RuntimeException("Can not create database in read only mode.");
        }

        if (username == null || username.length() == 0 ||
                password == null || password.length() == 0) {
            throw new RuntimeException(
                    "Must have valid username and password String");
        }

        try {
            // Create the conglomerate
            conglomerate.create(getName());

            DatabaseConnection connection = createNewConnection(null, null);
            DatabaseQueryContext context = new DatabaseQueryContext(connection);
            connection.getLockingMechanism().setMode(
                    LockingMechanism.EXCLUSIVE_MODE);
            connection.setCurrentSchema(SYSTEM_SCHEMA);

            // Create the schema information tables introduced in version 0.90
            // and 0.94
            createSchemaInfoTables(connection);

            // The system tables that are present in every conglomerate.
            createSystemTables(connection);
            // Create the system views
            createSystemViews(connection);

            // Creates the administrator user.
            createUser(context, username, password);
            // This is the admin user so add to the 'secure access' table.
            addUserToGroup(context, username, SECURE_GROUP);
            // Allow all localhost TCP connections.
            // NOTE: Permissive initial security!
            grantHostAccessToUser(context, username, "TCP", "%");
            // Allow all Local connections (from within JVM).
            grantHostAccessToUser(context, username, "Local", "%");

            // Sets the system grants for the administrator
            setSystemGrants(connection, username);

            // Set all default system procedures.
            setupSystemFunctions(connection, username);

            try {
                // Close and commit this transaction.
                connection.commit();
            } catch (TransactionException e) {
                Debug().writeException(e);
                throw new Error("Transaction Error: " + e.getMessage());
            }

            connection.getLockingMechanism().finishMode(
                    LockingMechanism.EXCLUSIVE_MODE);
            connection.close();

            // Close the conglomerate.
            conglomerate.close();

        } catch (DatabaseException e) {
            Debug().writeException(e);
            throw new Error("Database Exception: " + e.getMessage());
        } catch (IOException e) {
            Debug().writeException(e);
            throw new Error("IO Error: " + e.getMessage());
        }

    }

    /**
     * Initializes the database.  This opens all the files that are required for
     * the operation of the database.  If it finds that the version of the
     * data files are not a compatible version, this method throws an exception.
     * <p>
     * NOTE: Perhaps a better name for this method is 'open'.
     */
    public void init() throws DatabaseException {

        if (initialised) {
            throw new RuntimeException("Init() method can only be called once.");
        }

        // Reset all session statistics.
        stats().resetSession();

        try {
            File log_path = system.getLogDirectory();
            if (log_path != null && system.logQueries()) {
                commands_log = new Log(new File(log_path.getPath(), "commands.log"),
                        256 * 1024, 5);
            } else {
                commands_log = Log.nullLog();
            }

            // Check if the state file exists.  If it doesn't, we need to report
            // incorrect version.
            if (!storeSystem().storeExists(getName() + "_sf")) {
                // If state store doesn't exist but the legacy style '.sf' state file
                // exists,
                if (system.getDatabasePath() != null &&
                        new File(system.getDatabasePath(), getName() + ".sf").exists()) {
                    throw new DatabaseException(
                            "The state store for this database doesn't exist.  This means " +
                                    "the database version is pre version 1.0.  Please see the " +
                                    "README for the details for converting this database.");
                } else {
                    // If neither store or state file exist, assume database doesn't
                    // exist.
                    throw new DatabaseException("The database does not exist.");
                }
            }

            // Open the conglomerate
            conglomerate.open(getName());

            // Check the state of the conglomerate,
            DatabaseConnection connection = createNewConnection(null, null);
            DatabaseQueryContext context = new DatabaseQueryContext(connection);
            connection.getLockingMechanism().setMode(
                    LockingMechanism.EXCLUSIVE_MODE);
            if (!connection.tableExists(TableDataConglomerate.PERSISTENT_VAR_TABLE)) {
                throw new DatabaseException(
                        "The DatabaseVars table doesn't exist.  This means the " +
                                "database is pre-schema version 1 or the table has been deleted." +
                                "If you are converting an old version of the database, please " +
                                "convert the database using an older release.");
            }

            // What version is the data?
            DataTable database_vars =
                    connection.getTable(TableDataConglomerate.PERSISTENT_VAR_TABLE);
            Map vars = database_vars.toMap();
            String db_version = vars.get("database.version").toString();
            // If the version doesn't equal the current version, throw an error.
            if (!db_version.equals("1.4")) {
                throw new DatabaseException(
                        "Incorrect data file version '" + db_version + "'.  Please see " +
                                "the README on how to convert the data files to the current " +
                                "version.");
            }

            // Commit and close the connection.
            connection.commit();
            connection.getLockingMechanism().finishMode(
                    LockingMechanism.EXCLUSIVE_MODE);
            connection.close();

        } catch (TransactionException e) {
            // This would be very strange error to receive for in initializing
            // database...
            throw new Error("Transaction Error: " + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace(System.err);
            throw new Error("IO Error: " + e.getMessage());
        }

        // Sets up the system table listeners
        setSystemTableListeners();

        initialised = true;

    }

    /**
     * Cleanly shuts down the database.  It is important that this method is
     * called just before the system closes down.
     * <p>
     * The main purpose of this method is to ensure any tables that are backed
     * by files and in a 'safe' state and cleanly flushed to the file system.
     * <p>
     * If 'delete_on_shutdown' is true, the database will delete itself from the
     * file system when it shuts down.
     */
    public void shutdown() throws DatabaseException {

        if (initialised == false) {
            throw new Error("The database is not initialized.");
        }

        try {
            if (delete_on_shutdown == true) {
                // Delete the conglomerate if the database is set to delete on
                // shutdown.
                conglomerate.delete();
            } else {
                // Otherwise close the conglomerate.
                conglomerate.close();
            }
        } catch (IOException e) {
            Debug().writeException(e);
            throw new Error("IO Error: " + e.getMessage());
        }

        // Shut down the logs...
        if (commands_log != null) {
            commands_log.close();
        }

        initialised = false;

    }

    /**
     * Returns true if the database exists.  This must be called before 'init'
     * and 'create'.  It checks that the database files exist and we can boot
     * into the database.
     */
    public boolean exists() {
        if (initialised == true) {
            throw new RuntimeException(
                    "The database is initialised, so no point testing it's existance.");
        }

        try {
            // HACK: If the legacy style '.sf' state file exists then we must return
            //   true here because technically the database exists but is not in the
            //   correct version.
            if (conglomerate.exists(getName())) {
                return true;
            } else {
                boolean is_file_s_system =
                        (system.storeSystem() instanceof V1FileStoreSystem);
                return is_file_s_system &&
                        new File(system.getDatabasePath(), getName() + ".sf").exists();
            }
        } catch (IOException e) {
            Debug().writeException(e);
            throw new RuntimeException("IO Error: " + e.getMessage());
        }

    }

    /**
     * If the 'deleteOnShutdown' flag is set, the database will delete the
     * database from the file system when it is shutdown.
     * <p>
     * NOTE: Use with care - if this is set to true and the database is shutdown
     *   it will result in total loss of data.
     */
    public final void setDeleteOnShutdown(boolean status) {
        delete_on_shutdown = status;
    }


    /**
     * Returns true if the database is initialised.
     */
    public boolean isInitialized() {
        return initialised;
    }

    /**
     * Copies all the persistent data in this database (the conglomerate) to the
     * given destination path.  This can copy information while the database
     * is 'live'.
     */
    public void liveCopyTo(File path) throws IOException {
        if (initialised == false) {
            throw new Error("The database is not initialized.");
        }

        // Set up the destination conglomerate to copy all the data to,
        // Note that this sets up a typical destination conglomerate and changes
        // the cache size and disables the debug log.
        TransactionSystem copy_system = new TransactionSystem();
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(path.getAbsolutePath());
        config.setLogPath("");
        config.setMinimumDebugLevel(50000);
        // Set data cache to 1MB
        config.setValue("data_cache_size", "1048576");
        // Set io_safety_level to 1 for destination database
        // ISSUE: Is this a good assumption to make -
        //     we don't care if changes are lost by a power failure when we are
        //     backing up the database.  Even if journalling is enabled, a power
        //     failure will lose changes in the backup copy anyway.
        config.setValue("io_safety_level", "1");
        java.io.StringWriter debug_output = new java.io.StringWriter();
        copy_system.setDebugOutput(debug_output);
        copy_system.init(config);
        final TableDataConglomerate dest_conglomerate =
                new TableDataConglomerate(copy_system, copy_system.storeSystem());

        // Open the congloemrate
        dest_conglomerate.minimalCreate("DefaultDatabase");

        try {
            // Make a copy of this conglomerate into the destination conglomerate,
            conglomerate.liveCopyTo(dest_conglomerate);
        } finally {
            // Close the congloemrate when finished.
            dest_conglomerate.close();
            // Dispose the TransactionSystem
            copy_system.dispose();
        }

    }

    // ---------- Database convertion ----------

    /**
     * Processes each table in user space and converts the format to the newest
     * version of the data file format.  This is simply achieved by running the
     * 'compactTable' command on the transaction for each table.
     */
    private void convertAllUserTables(DatabaseConnection connection,
                                      PrintStream out) throws TransactionException {
        out.println("Converting user table format to latest version.");
        // Convert all user tables in the database
        TableName[] all_tables = connection.getTableList();
        for (int i = 0; i < all_tables.length; ++i) {
            TableName table_name = all_tables[i];
            String schema_name = table_name.getSchema();
            if (!schema_name.equals("SYS_INFO") &&
                    connection.getTableType(table_name).equals("TABLE")) {
                out.println("Converting: " + table_name);
                connection.compactTable(table_name);
                connection.commit();
            }
        }
    }

    /**
     * Returns true if the given sql type is possibly a large object.
     */
    private static boolean largeObjectTest(int sql_type) {
        return (sql_type == SQLTypes.CHAR ||
                sql_type == SQLTypes.VARCHAR ||
                sql_type == SQLTypes.LONGVARCHAR ||
                sql_type == SQLTypes.BINARY ||
                sql_type == SQLTypes.VARBINARY ||
                sql_type == SQLTypes.LONGVARBINARY ||
                sql_type == SQLTypes.BLOB ||
                sql_type == SQLTypes.CLOB);
    }

    /**
     * Scans all the user tables for large objects and if a large object is
     * found, it is moved into the BlobStore.  A large object is an object that
     * uses more than 16 kbytes of storage space.
     */
    private void moveLargeObjectsToBlobStore(DatabaseConnection connection,
                                             PrintStream out)
            throws TransactionException, IOException, DatabaseException {
        out.println("Scanning user tables for large objects.");

        DatabaseQueryContext context = new DatabaseQueryContext(connection);
        BlobStore blob_store = conglomerate.getBlobStore();

        // Scan all user tables in the database
        TableName[] all_tables = connection.getTableList();
        for (int i = 0; i < all_tables.length; ++i) {
            TableName table_name = all_tables[i];
            String schema_name = table_name.getSchema();
            boolean table_changed = false;

            if (!schema_name.equals("SYS_INFO") &&
                    connection.getTableType(table_name).equals("TABLE")) {

                out.println("Processing: " + table_name);
                DataTable table = connection.getTable(table_name);
                DataTableDef table_def = table.getDataTableDef();

                boolean possibly_has_large_objects = false;
                int column_count = table_def.columnCount();
                for (int n = 0; n < column_count; ++n) {
                    int sql_type = table_def.columnAt(n).getSQLType();
                    if (largeObjectTest(sql_type)) {
                        possibly_has_large_objects = true;
                    }
                }

                if (possibly_has_large_objects) {

                    RowEnumeration e = table.rowEnumeration();
                    while (e.hasMoreRows()) {

                        int row_index = e.nextRowIndex();
                        ArrayList changes = new ArrayList(4);

                        for (int p = 0; p < column_count; ++p) {
                            DataTableColumnDef col_def = table_def.columnAt(p);
                            int sql_type = col_def.getSQLType();

                            if (largeObjectTest(sql_type)) {
                                TObject tob = table.getCellContents(p, row_index);
                                Object ob = tob.getObject();
                                if (ob != null) {
                                    // String type
                                    if (ob instanceof StringObject) {
                                        StringObject s_object = (StringObject) ob;
                                        if (s_object.length() > 4 * 1024) {
                                            ClobRef ref =
                                                    blob_store.putStringInBlobStore(s_object.toString());
                                            changes.add(new Assignment(
                                                    new Variable(table_name, col_def.getName()),
                                                    new Expression(
                                                            new TObject(tob.getTType(), ref))));
                                        }
                                    }
                                    // Binary type
                                    if (ob instanceof ByteLongObject) {
                                        ByteLongObject b_object = (ByteLongObject) ob;
                                        if (b_object.length() > 8 * 1024) {
                                            BlobRef ref =
                                                    blob_store.putByteLongObjectInBlobStore(b_object);
                                            changes.add(new Assignment(
                                                    new Variable(table_name, col_def.getName()),
                                                    new Expression(
                                                            new TObject(tob.getTType(), ref))));
                                        }
                                    }
                                }
                            }
                        }

                        // If there was a change
                        if (changes.size() > 0) {
                            // Update the row
                            Assignment[] assignments = (Assignment[]) changes.toArray(
                                    new Assignment[changes.size()]);
                            Table st = table.singleRowSelect(row_index);
                            table.update(context, st, assignments, -1);
                            table_changed = true;
                        }

                    }  // For each row

                    if (table_changed) {
                        // Commit the connection.
                        connection.commit();
                        // Compact this table (will remove space from large objects).
                        connection.compactTable(table_name);
                    }

                    // Commit the connection.
                    connection.commit();

                }
            }
        }
    }

    /**
     * Functionality for converting and old database format to the existing
     * format.  This would typically be called from a convert tool program.
     * <p>
     * Returns true if the convert was successful or false if it wasn't (error
     * message is output to the PrintWriter).
     */
    public boolean convertToCurrent(PrintStream out, String admin_username)
            throws IOException {

        // Reset all session statistics.
        stats().resetSession();

        try {
            // Don't log commands (there shouldn't be any anyway).
            commands_log = Log.nullLog();

            // Convert the state file if it is necessary.
            File legacy_state_file =
                    new File(system.getDatabasePath(), getName() + ".sf");
            if (legacy_state_file.exists()) {
                String state_store_fn = getName() + "_sf";
                // If the state store file already exists
                if (storeSystem().storeExists(state_store_fn)) {
                    throw new IOException(
                            "Both legacy and version 1 state file exist.  Please remove one.");
                }
                out.println("Converting state file to current version.");
                // Create the new store,
                Store new_ss = storeSystem().createStore(state_store_fn);
                StateStore ss = new StateStore(new_ss);
                // Convert the existing store
                long new_p = ss.convert(legacy_state_file, Debug());
                // Set the fixed area in the store to point to this new structure
                MutableArea fixed_area = new_ss.getMutableArea(-1);
                fixed_area.putLong(new_p);
                fixed_area.checkOut();
                // Flush the changes to the new store and close
                storeSystem().closeStore(new_ss);
                // Delete the old state file.
                legacy_state_file.delete();
                out.println("State store written.");
            }

            out.println("Opening conglomerate.");

            // Open the conglomerate
            conglomerate.open(getName());

            // Check the state of the conglomerate,
            DatabaseConnection connection = createNewConnection(null, null);
            DatabaseQueryContext context = new DatabaseQueryContext(connection);
            connection.getLockingMechanism().setMode(LockingMechanism.EXCLUSIVE_MODE);
            if (!connection.tableExists(TableDataConglomerate.PERSISTENT_VAR_TABLE)) {
                out.println(
                        "The DatabaseVars table doesn't exist.  This means the " +
                                "database is pre-schema version 1 or the table has been deleted." +
                                "If you are converting an old version of the database, please " +
                                "convert the database using an older release.");
                return false;
            }

            // Check the user given exists
            if (!userExists(context, admin_username)) {
                out.println(
                        "The admin username given (" + admin_username +
                                ") does not exist in this database so I am unable to convert the " +
                                "database.");
                return false;
            }

            // What version is the data?
            DataTable database_vars =
                    connection.getTable(TableDataConglomerate.PERSISTENT_VAR_TABLE);
            Map vars = database_vars.toMap();
            String db_version = vars.get("database.version").toString();
            if (db_version.equals("1.0")) {
                // Convert from 1.0 to 1.4
                out.println("Version 1.0 found.");
                out.println("Converting database to version 1.4 schema...");

                try {
                    // Drop the tables that were deprecated
                    connection.dropTable(new TableName(SYSTEM_SCHEMA, "PrivAdd"));
                    connection.dropTable(new TableName(SYSTEM_SCHEMA, "PrivAlter"));
                    connection.dropTable(new TableName(SYSTEM_SCHEMA, "PrivRead"));
                } catch (Error e) { /* ignore */ }

                // Reset the sequence id for the tables.
                conglomerate.resetAllSystemTableID();

                // Create/Update the conglomerate level tables.
                conglomerate.updateSystemTableSchema();

                // Commit the changes so far.
                connection.commit();

                // Create/Update the system tables that are present in every
                // conglomerate.
                createSystemTables(connection);

                // Commit the changes so far.
                connection.commit();

                // Creating the system JDBC system schema
                connection.createSchema(JDBC_SCHEMA, "SYSTEM");
                // Create the system views
                createSystemViews(connection);

                // Sets the system grants for the administrator
                setSystemGrants(connection, admin_username);
                // Sets the table grants for the administrator
                convertPreGrant(connection, admin_username);

                // Allow all localhost TCP connections.
                // NOTE: Permissive initial security!
                grantHostAccessToUser(context, admin_username, "TCP", "%");
                // Allow all Local connections (from within JVM).
                grantHostAccessToUser(context, admin_username, "Local", "%");

                // Convert all tables in the database to the current table format.
                convertAllUserTables(connection, out);

                // Move any large binary or string objects into the blob store.
                moveLargeObjectsToBlobStore(connection, out);

                // Set all default system procedures.
                setupSystemFunctions(connection, admin_username);

                // Commit the changes so far.
                connection.commit();

                // Update to version 1.4
                database_vars =
                        connection.getTable(TableDataConglomerate.PERSISTENT_VAR_TABLE);
                updateDatabaseVars(context, database_vars, "database.version", "1.4");
                db_version = "1.4";

            } else if (db_version.equals("1.1")) {
                // Convert from 1.1 to 1.4
                out.println("Version 1.1 found.");
                out.println("Converting database to version 1.4 schema...");

                // Reset the sequence id for the tables.
                conglomerate.resetAllSystemTableID();

                // Create/Update the conglomerate level tables.
                conglomerate.updateSystemTableSchema();

                // Commit the changes so far.
                connection.commit();

                // Create/Update the system tables that are present in every
                // conglomerate.
                createSystemTables(connection);

                // Commit the changes so far.
                connection.commit();
                // Update the 'database_vars' table.
                database_vars =
                        connection.getTable(TableDataConglomerate.PERSISTENT_VAR_TABLE);

                // Creating the system JDBC system schema
                connection.createSchema(JDBC_SCHEMA, "SYSTEM");
                // Create the system views
                createSystemViews(connection);

                // Clear all grants.
                clearAllGrants(connection);

                // Sets the system grants for the administrator
                setSystemGrants(connection, admin_username);
                // Sets the table grants for the administrator
                convertPreGrant(connection, admin_username);

                // Convert all tables in the database to the current table format.
                convertAllUserTables(connection, out);

                // Move any large binary or string objects into the blob store.
                moveLargeObjectsToBlobStore(connection, out);

                // Set all default system procedures.
                setupSystemFunctions(connection, admin_username);

                // Commit the changes so far.
                connection.commit();

                // Update to version 1.4
                database_vars =
                        connection.getTable(TableDataConglomerate.PERSISTENT_VAR_TABLE);
                updateDatabaseVars(context, database_vars, "database.version", "1.4");
                db_version = "1.4";

            } else if (db_version.equals("1.2")) {
                // Convert from 1.2 to 1.4
                out.println("Version 1.2 found.");
                out.println("Converting database to version 1.4 schema...");

                // Create/Update the conglomerate level tables.
                conglomerate.updateSystemTableSchema();

                // Commit the changes so far.
                connection.commit();

                // Create/Update the system tables that are present in every
                // conglomerate.
                createSystemTables(connection);

                // Commit the changes so far.
                connection.commit();

                // Convert all tables in the database to the current table format.
                convertAllUserTables(connection, out);

                // Move any large binary or string objects into the blob store.
                moveLargeObjectsToBlobStore(connection, out);

                // Commit the changes so far.
                connection.commit();

                // Set all default system procedures.
                setupSystemFunctions(connection, admin_username);

                // Commit the changes so far.
                connection.commit();

                // Update to version 1.4
                database_vars =
                        connection.getTable(TableDataConglomerate.PERSISTENT_VAR_TABLE);
                updateDatabaseVars(context, database_vars, "database.version", "1.4");
                db_version = "1.4";

            } else if (db_version.equals("1.3")) {
                out.println("Version 1.3 found.");
                out.println("Converting database to version 1.4 schema...");

                // Create/Update the conglomerate level tables.
                conglomerate.updateSystemTableSchema();

                // Commit the changes so far.
                connection.commit();

                // Create/Update the system tables that are present in every
                // conglomerate.
                createSystemTables(connection);

                // Commit the changes so far.
                connection.commit();

                // Drop the 'SystemTrigger' table that was erroniously added in 1.3
                try {
                    connection.dropTable(new TableName(SYSTEM_SCHEMA, "SystemTrigger"));
                } catch (Error e) { /* ignore */ }

                // Set all default system procedures.
                setupSystemFunctions(connection, admin_username);

                // Commit the changes so far.
                connection.commit();

                // Update to version 1.4
                database_vars =
                        connection.getTable(TableDataConglomerate.PERSISTENT_VAR_TABLE);
                updateDatabaseVars(context, database_vars, "database.version", "1.4");
                db_version = "1.4";

            } else if (db_version.equals("1.4")) {
                out.println("Version 1.4 found.");
                out.println("Version of data files is current.");
            } else if (!db_version.equals("1.4")) {
                // This means older versions of the database will not support the data
                // format of newer versions.
                out.println("Version " + db_version + " found.");
                out.println("This is not a recognized version number and can not be " +
                        "converted.  Perhaps this is a future version?  I can " +
                        "not convert backwards from a future version.");
                return false;
            }

            // Commit and close the connection.
            connection.commit();
            connection.getLockingMechanism().finishMode(
                    LockingMechanism.EXCLUSIVE_MODE);
            connection.close();
            return true;

        } catch (TransactionException e) {
            // This would be very strange error to receive for in initializing
            // database...
            out.println("Transaction Error: " + e.getMessage());
            e.printStackTrace(out);
            return false;
        } catch (DatabaseException e) {
            out.println("Database Error: " + e.getMessage());
            e.printStackTrace(out);
            return false;
        } finally {
            try {
                conglomerate.close();
            } catch (Throwable e) {
                // ignore
            }
        }

    }

    // ---------- Server side procedures ----------

    /**
     * Resolves a procedure name into a DBProcedure object.  This is used for
     * finding a server side script.  It throws a DatabaseException if the
     * procedure could not be resolved or there was an error retrieving it.
     * <p>
     * ISSUE: Move this to DatabaseSystem?
     */
    public DatabaseProcedure getDBProcedure(
            String procedure_name, DatabaseConnection connection)
            throws DatabaseException {

        // The procedure we are getting.
        DatabaseProcedure procedure_instance;

        // See if we can find the procedure as a .js (JavaScript) file in the
        // procedure resources.
        String p = "/" + procedure_name.replace('.', '/');
        // If procedure doesn't starts with '/com/pony/procedure/' then add it
        // on here.
        if (!p.startsWith("/com/pony/procedure/")) {
            p = "/com/pony/procedure/" + p;
        }
        p = p + ".js";

        // Is there a resource available?
        java.net.URL url = getClass().getResource(p);

        if (url != null) {
            // Create a server side procedure for the .js file
            //   ( This code is not included in the free release )
            procedure_instance = null;

        } else {
            try {
                // Couldn't find the javascript script, so try and resolve as an
                // actual Java class file.
                // Find the procedure
                Class proc = Class.forName("com.pony.procedure." + procedure_name);
                // Instantiate a new instance of the procedure
                procedure_instance = (DatabaseProcedure) proc.newInstance();

                Debug().write(Lvl.INFORMATION, this,
                        "Getting raw Java class file: " + procedure_name);
            } catch (IllegalAccessException e) {
                Debug().writeException(e);
                throw new DatabaseException("Illegal Access: " + e.getMessage());
            } catch (InstantiationException e) {
                Debug().writeException(e);
                throw new DatabaseException("Instantiation Error: " + e.getMessage());
            } catch (ClassNotFoundException e) {
                Debug().writeException(e);
                throw new DatabaseException("Class Not Found: " + e.getMessage());
            }
        }

        // Return the procedure.
        return procedure_instance;

    }

    // ---------- System access ----------

    /**
     * Returns the DatabaseSystem that this Database is from.
     */
    public final DatabaseSystem getSystem() {
        return system;
    }

    /**
     * Returns the StoreSystem for this Database.
     */
    public final StoreSystem storeSystem() {
        return system.storeSystem();
    }

    /**
     * Convenience static for accessing the global Stats object.  Perhaps this
     * should be deprecated?
     */
    public final Stats stats() {
        return getSystem().stats();
    }

    /**
     * Returns the DebugLogger implementation from the DatabaseSystem.
     */
    public final DebugLogger Debug() {
        return getSystem().Debug();
    }

    /**
     * Returns the system trigger manager.
     */
    public final TriggerManager getTriggerManager() {
        return trigger_manager;
    }

    /**
     * Returns the system user manager.
     */
    public final UserManager getUserManager() {
        return getSystem().getUserManager();
    }

    /**
     * Creates an event for the database dispatcher.
     */
    public final Object createEvent(Runnable runner) {
        return getSystem().createEvent(runner);
    }

    /**
     * Posts an event on the database dispatcher.
     */
    public final void postEvent(int time, Object event) {
        getSystem().postEvent(time, event);
    }

    /**
     * Returns the system DataCellCache.
     */
    public final DataCellCache getDataCellCache() {
        return getSystem().getDataCellCache();
    }

    /**
     * Returns true if the database has shut down.
     */
    public final boolean hasShutDown() {
        return getSystem().hasShutDown();
    }

    /**
     * Starts the shutdown thread which should contain delegates that shut the
     * database and all its resources down.  This method returns immediately.
     */
    public final void startShutDownThread() {
        getSystem().startShutDownThread();
    }

    /**
     * Blocks until the database has shut down.
     */
    public final void waitUntilShutdown() {
        getSystem().waitUntilShutdown();
    }

    /**
     * Executes database functions from the 'run' method of the given runnable
     * instance on the first available worker thread.  All database functions
     * must go through a worker thread.  If we ensure this, we can easily stop
     * all database functions from executing if need be.  Also, we only need to
     * have a certain number of threads active at any one time rather than a
     * unique thread for each connection.
     */
    public final void execute(User user, DatabaseConnection database,
                              Runnable runner) {
        getSystem().execute(user, database, runner);
    }

    /**
     * Registers the delegate that is executed when the shutdown thread is
     * activated.
     */
    public final void registerShutDownDelegate(Runnable delegate) {
        getSystem().registerShutDownDelegate(delegate);
    }

    /**
     * Controls whether the database is allowed to execute commands or not.  If
     * this is set to true, then calls to 'execute' will be executed
     * as soon as there is a free worker thread available.  Otherwise no
     * commands are executed until this is enabled.
     */
    public final void setIsExecutingCommands(boolean status) {
        getSystem().setIsExecutingCommands(status);
    }

    /**
     * Returns a static table that has a single row but no columns.  This table
     * is useful for certain database operations.
     */
    public final Table getSingleRowTable() {
        return SINGLE_ROW_TABLE;
    }


    // ---------- Static methods ----------

    /**
     * Given the DatabaseVars table, this will update the given key with
     * the given value in the table in the current transaction.
     */
    private static void updateDatabaseVars(QueryContext context,
                                           DataTable database_vars, String key, String value)
            throws DatabaseException {
        // The references to the first and second column (key/value)
        Variable c1 = database_vars.getResolvedVariable(0); // First column
        Variable c2 = database_vars.getResolvedVariable(1); // Second column

        // Assignment: second column = value
        Assignment assignment = new Assignment(c2,
                new Expression(TObject.stringVal(value)));
        // All rows from database_vars where first column = the key
        Table t1 = database_vars.simpleSelect(context, c1, Operator.get("="),
                new Expression(TObject.stringVal(key)));

        // Update the variable
        database_vars.update(context, t1, new Assignment[]{assignment}, -1);

    }


    public void finalize() throws Throwable {
        super.finalize();
        if (isInitialized()) {
            System.err.println("Database object was finalized and is initialized!");
        }
    }

}
