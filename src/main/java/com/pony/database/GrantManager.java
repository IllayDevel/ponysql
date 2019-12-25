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

import com.pony.util.IntegerVector;
import com.pony.util.BigNumber;
import com.pony.util.Cache;

/**
 * A class that manages the grants on a database for a given database
 * connection and user.
 *
 * @author Tobias Downer
 */

public class GrantManager {

    // ---------- Statics ----------

    /**
     * Represents a TABLE object to grant privs over for the user.
     */
    public final static int TABLE = 1;

    /**
     * Represents a DOMAIN object to grant privs over for the user.
     */
    public final static int DOMAIN = 2;

//  /**
//   * Represents a STORED PROCEDURE object to grant privs over for this user.
//   */
//  public final static int STORED_PROCEDURE = 16;
//
//  /**
//   * Represents a TRIGGER object to grant privs over for this user.
//   */
//  public final static int TRIGGER = 17;
//
//  /**
//   * Represents a custom SEQUENCE GENERATOR object to grant privs over.
//   */
//  public final static int SEQUENCE_GENERATOR = 18;

    /**
     * Represents a SCHEMA object to grant privs over for the user.
     */
    public final static int SCHEMA = 65;

    /**
     * Represents a CATALOG object to grant privs over for this user.
     */
    public final static int CATALOG = 66;


    /**
     * The string representing the public user (privs granted to all users).
     */
    public final static String PUBLIC_USERNAME_STR = "@PUBLIC";

    /**
     * The name of the 'public' username.  If a grant is made on 'public' then
     * all users are given the grant.
     */
    public final static TObject PUBLIC_USERNAME =
            TObject.stringVal(PUBLIC_USERNAME_STR);

    // ---------- Members ----------

    /**
     * The DatabaseConnection instance.
     */
    private final DatabaseConnection connection;

    /**
     * The QueryContext instance.
     */
    private final QueryContext context;

    /**
     * A cache of privileges for the various tables in the database.  This cache
     * is populated as the user 'visits' a table.
     */
    private final Cache priv_cache;

    /**
     * Set to true if the grant table is modified in this manager.
     */
    private boolean grant_table_changed;


    /**
     * Constructs the GrantManager.
     * Should only be constructed from DatabaseConnection.
     */
    GrantManager(DatabaseConnection connection) {
        this.connection = connection;
        this.context = new DatabaseQueryContext(connection);
        this.priv_cache = new Cache(129, 129, 20);

        this.grant_table_changed = false;

        // Attach a cache backed on the GRANTS table which will invalidate the
        // connection cache whenever the grant table is modified.
        connection.attachTableBackedCache(new TableBackedCache(Database.SYS_GRANTS) {
            public void purgeCacheOfInvalidatedEntries(
                    IntegerVector added_rows, IntegerVector removed_rows) {
                // If there were changed then invalidate the cache
                if (grant_table_changed) {
                    invalidateGrantCache();
                    grant_table_changed = false;
                }
                // Otherwise, if there were committed added or removed changes also
                // invalidate the cache,
                else if ((added_rows != null && added_rows.size() > 0) ||
                        (removed_rows != null && removed_rows.size() > 0)) {
                    invalidateGrantCache();
                }
            }
        });
    }

    // ---------- Private priv caching methods ----------

    /**
     * Flushes any grant information that's being cached.
     */
    private void invalidateGrantCache() {
        priv_cache.removeAll();
    }

    /**
     * Inner class that represents a grant query on a particular object, param
     * and user name.
     * <p>
     * This object is designed to be an immutable key in a cache.
     */
    private static class GrantQuery {
        private final int object;
        private final String param;
        private final String username;
        private int flags;

        GrantQuery(int object, String param, String username,
                   boolean flag1, boolean flag2) {
            this.object = object;
            this.param = param;
            this.username = username;
            this.flags = flag1 ? 1 : 0;
            this.flags = this.flags | (flag2 ? 2 : 0);
        }

        public boolean equals(Object ob) {
            GrantQuery dest = (GrantQuery) ob;
            return (object == dest.object &&
                    param.equals(dest.param) &&
                    username.equals(dest.username) &&
                    flags == dest.flags);
        }

        public int hashCode() {
            return object + param.hashCode() + username.hashCode() + flags;
        }

    }


    private Privileges getPrivs(int object, String param, String username,
                                boolean only_grant_options,
                                String granter, boolean include_public_privs)
            throws DatabaseException {

        // Create the grant query key
        GrantQuery key = new GrantQuery(object, param, username,
                only_grant_options, include_public_privs);

        // Is the Privileges object for this query already in the cache?
        Privileges privs = (Privileges) priv_cache.get(key);
        if (privs == null) {
            // Not in cache so we need to ask database for the information.

//      try {
//        Connection c = connection.getJDBCConnection();
//        PreparedStatement stmt = c.prepareStatement(
//          " SELECT \"priv\" FROM \"SYS_INFO.GrantPriv\" " +
//          "  WHERE \"grant_id\" IN " +
//          "    ( SELECT \"id\" FROM \"SYS_INFO.Grant\" " +
//          "       WHERE \"param\" = ? " +
//          "         AND \"object\" = ? " +
//          "         AND (\"grantor\" = ? OR (? AND \"grantor\" = '@PUBLIC')) " +
//          "         AND (? OR \"grant_option\" = 'true') " +
//          "         AND (? OR \"granter\" = ?) " +
//          "    )");
//        stmt.setString(1, param);
//        stmt.setInt(2, object);
//        stmt.setString(3, username);
//        stmt.setBoolean(4, include_public_privs);
//        stmt.setBoolean(5, !only_grant_options);
//        stmt.setBoolean(6, (granter == null));
//        stmt.setString(7, granter);
//        ResultSet rs = stmt.executeQuery();
//        privs = Privileges.fromResultSet(rs);
//        rs.close();
//        stmt.close();
//        c.close();
//      }
//      catch (SQLException e) {
//        connection.Debug().writeException(e);
//        throw new DatabaseException("SQL Error: " + e.getMessage());
//      }

            // The system grants table.
            DataTable grant_table = connection.getTable(Database.SYS_GRANTS);

            Variable object_col = grant_table.getResolvedVariable(1);
            Variable param_col = grant_table.getResolvedVariable(2);
            Variable grantee_col = grant_table.getResolvedVariable(3);
            Variable grant_option_col = grant_table.getResolvedVariable(4);
            Variable granter_col = grant_table.getResolvedVariable(5);
            Operator EQUALS = Operator.get("=");

            Table t1 = grant_table;

            // All that match the given object parameter
            // It's most likely this will reduce the search by the most so we do
            // it first.
            t1 = t1.simpleSelect(context, param_col, EQUALS,
                    new Expression(TObject.stringVal(param)));

            // The next is a single exhaustive select through the remaining records.
            // It finds all grants that match either public or the grantee is the
            // username, and that match the object type.

            // Expression: ("grantee_col" = username OR "grantee_col" = 'public')
            Expression user_check =
                    Expression.simple(grantee_col, EQUALS, TObject.stringVal(username));
            if (include_public_privs) {
                user_check = new Expression(
                        user_check, Operator.get("or"),
                        Expression.simple(grantee_col, EQUALS, PUBLIC_USERNAME)
                );
            }
            // Expression: ("object_col" = object AND
            //              ("grantee_col" = username OR "grantee_col" = 'public'))
            // All that match the given username or public and given object
            Expression expr = new Expression(
                    Expression.simple(object_col, EQUALS, TObject.intVal(object)),
                    Operator.get("and"),
                    user_check);

            // Are we only searching for grant options?
            if (only_grant_options) {
                Expression grant_option_check =
                        Expression.simple(grant_option_col, EQUALS,
                                TObject.stringVal("true"));
                expr = new Expression(expr, Operator.get("and"), grant_option_check);
            }

            // Do we need to check for a granter when we looking for privs?
            if (granter != null) {
                Expression granter_check =
                        Expression.simple(granter_col, EQUALS, TObject.stringVal(granter));
                expr = new Expression(expr, Operator.get("and"), granter_check);
            }

            t1 = t1.exhaustiveSelect(context, expr);

            // For each grant, merge with the resultant priv object
            privs = Privileges.EMPTY_PRIVS;
            RowEnumeration e = t1.rowEnumeration();
            while (e.hasMoreRows()) {
                int row_index = e.nextRowIndex();
                BigNumber priv_bit =
                        (BigNumber) t1.getCellContents(0, row_index).getObject();
                privs = privs.add(priv_bit.intValue());
            }

            // Put the privs object in the cache
            priv_cache.put(key, privs);

        }

        return privs;
    }

    /**
     * Internal method that sets the privs for the given object, param, grantee,
     * grant option and granter.  This first revokes any grants that have been
     * setup for the object, and adds a new record with the new grants.
     */
    private void internalSetPrivs(Privileges new_privs, int object, String param,
                                  String grantee, boolean grant_option, String granter)
            throws DatabaseException {

        // Revoke existing privs on this object for this grantee
        revokeAllGrantsOnObject(object, param, grantee, grant_option, granter);

        if (!new_privs.isEmpty()) {

            // The system grants table.
            DataTable grant_table = connection.getTable(Database.SYS_GRANTS);

            // Add the grant to the grants table.
            RowData rdat = new RowData(grant_table);
            rdat.setColumnDataFromObject(0, BigNumber.fromInt(new_privs.toInt()));
            rdat.setColumnDataFromObject(1, BigNumber.fromInt(object));
            rdat.setColumnDataFromObject(2, param);
            rdat.setColumnDataFromObject(3, grantee);
            rdat.setColumnDataFromObject(4, grant_option ? "true" : "false");
            rdat.setColumnDataFromObject(5, granter);
            grant_table.add(rdat);

            // Invalidate the privilege cache
            invalidateGrantCache();

            // Notify that the grant table has changed.
            grant_table_changed = true;

        }

    }

    // ---------- Public methods ----------

    /**
     * Adds a grant on the given database object.
     *
     * @param privs the privileges to grant.
     * @param object the object to grant (TABLE, DOMAIN, etc)
     * @param param the parameter of the object (eg. the table name)
     * @param grantee the user name to grant the privs to.
     * @param grant_option if true, allows the user to pass grants to other
     *                     users.
     * @param granter the user granting.
     */
    public void addGrant(Privileges privs, int object, String param,
                         String grantee, boolean grant_option, String granter)
            throws DatabaseException {

        if (object == TABLE) {
            // Check that the table exists,
            if (!connection.tableExists(TableName.resolve(param))) {
                throw new DatabaseException("Table: " + param + " does not exist.");
            }
        } else if (object == SCHEMA) {
            // Check that the schema exists.
            if (!connection.schemaExists(param)) {
                throw new DatabaseException("Schema: " + param + " does not exist.");
            }
        }

        // Get any existing grants on this object to this grantee
        Privileges existing_privs =
                getPrivs(object, param, grantee, grant_option, granter, false);
        // Merge the existing privs with the new privs being added.
        Privileges new_privs = privs.merge(existing_privs);

        // If the new_privs are the same as the existing privs, don't bother
        // changing anything.
        if (!new_privs.equals(existing_privs)) {
            internalSetPrivs(new_privs, object, param, grantee,
                    grant_option, granter);
        }

    }

    /**
     * For all tables in the given schema, this adds the given grant for each
     * of the tables.
     */
    public void addGrantToAllTablesInSchema(String schema, Privileges privs,
                                            String grantee, boolean grant_option,
                                            String granter) throws DatabaseException {
        // The list of all tables
        TableName[] list = connection.getTableList();
        for (int i = 0; i < list.length; ++i) {
            TableName tname = list[i];
            // If the table is in the given schema,
            if (tname.getSchema().equals(schema)) {
                addGrant(privs, TABLE, tname.toString(), grantee,
                        grant_option, granter);
            }
        }
    }

    /**
     * Removes a grant on the given object for the given grantee, grant option
     * and granter.
     */
    public void removeGrant(Privileges privs, int object, String param,
                            String grantee, boolean grant_option, String granter)
            throws DatabaseException {

        // Get any existing grants on this object to this grantee
        Privileges existing_privs =
                getPrivs(object, param, grantee, grant_option, granter, false);
        // Remove privs from the the existing privs.
        Privileges new_privs = existing_privs.remove(privs);

        // If the new_privs are the same as the existing privs, don't bother
        // changing anything.
        if (!new_privs.equals(existing_privs)) {
            internalSetPrivs(new_privs, object, param, grantee,
                    grant_option, granter);
        }

    }

    /**
     * Removes all privs granted on the given object for the given grantee with
     * the given grant option.
     */
    public void revokeAllGrantsOnObject(int object, String param,
                                        String grantee, boolean grant_option, String granter)
            throws DatabaseException {
        // The system grants table.
        DataTable grant_table = connection.getTable(Database.SYS_GRANTS);

        Variable object_col = grant_table.getResolvedVariable(1);
        Variable param_col = grant_table.getResolvedVariable(2);
        Variable grantee_col = grant_table.getResolvedVariable(3);
        Variable grant_option_col = grant_table.getResolvedVariable(4);
        Variable granter_col = grant_table.getResolvedVariable(5);
        Operator EQUALS = Operator.get("=");

        Table t1 = grant_table;

        // All that match the given object parameter
        // It's most likely this will reduce the search by the most so we do
        // it first.
        t1 = t1.simpleSelect(context, param_col, EQUALS,
                new Expression(TObject.stringVal(param)));

        // The next is a single exhaustive select through the remaining records.
        // It finds all grants that match either public or the grantee is the
        // username, and that match the object type.

        // Expression: ("grantee_col" = username)
        Expression user_check =
                Expression.simple(grantee_col, EQUALS, TObject.stringVal(grantee));
        // Expression: ("object_col" = object AND
        //              "grantee_col" = username)
        // All that match the given username or public and given object
        Expression expr = new Expression(
                Expression.simple(object_col, EQUALS, TObject.intVal(object)),
                Operator.get("and"),
                user_check);

        // Are we only searching for grant options?
        Expression grant_option_check =
                Expression.simple(grant_option_col, EQUALS,
                        TObject.stringVal(grant_option ? "true" : "false"));
        expr = new Expression(expr, Operator.get("and"), grant_option_check);

        // Make sure the granter matches up also
        Expression granter_check =
                Expression.simple(granter_col, EQUALS, TObject.stringVal(granter));
        expr = new Expression(expr, Operator.get("and"), granter_check);

        t1 = t1.exhaustiveSelect(context, expr);

        // Remove these rows from the table
        grant_table.delete(t1);

        // Invalidate the privilege cache
        invalidateGrantCache();

        // Notify that the grant table has changed.
        grant_table_changed = true;

    }

    /**
     * Completely removes all privs granted on the given object for all users.
     * This would typically be used when the object is dropped from the database.
     */
    public void revokeAllGrantsOnObject(int object, String param)
            throws DatabaseException {
        // The system grants table.
        DataTable grant_table = connection.getTable(Database.SYS_GRANTS);

        Variable object_col = grant_table.getResolvedVariable(1);
        Variable param_col = grant_table.getResolvedVariable(2);
        // All that match the given object
        Table t1 = grant_table.simpleSelect(context, object_col,
                Operator.get("="), new Expression(TObject.intVal(object)));
        // All that match the given parameter
        t1 = t1.simpleSelect(context,
                param_col, Operator.get("="),
                new Expression(TObject.stringVal(param)));

        // Remove these rows from the table
        grant_table.delete(t1);

        // Invalidate the privilege cache
        invalidateGrantCache();

        // Notify that the grant table has changed.
        grant_table_changed = true;

    }

    /**
     * Returns all Privileges for the given object for the given grantee (user).
     * This would be used to determine the access a user has to a table.
     * <p>
     * Note that the Privileges object includes all the grants on the object given
     * to PUBLIC also.
     * <p>
     * This method will concatenate multiple privs granted on the same
     * object.
     * <p>
     * PERFORMANCE: This method is called a lot (at least once on every query).
     */
    public Privileges userGrants(int object, String param, String username)
            throws DatabaseException {
        return getPrivs(object, param, username, false, null, true);
    }

    /**
     * Returns all Privileges for the given object for the given grantee (user)
     * that the user is allowed to give grant options for.  This would be used to
     * determine if a user has privs to give another user grants on an object.
     * <p>
     * Note that the Privileges object includes all the grants on the object given
     * to PUBLIC also.
     * <p>
     * This method will concatenate multiple grant options given on the same
     * object to the user.
     */
    public Privileges userGrantOptions(int object, String param, String username)
            throws DatabaseException {
        return getPrivs(object, param, username, true, null, true);
    }

}
