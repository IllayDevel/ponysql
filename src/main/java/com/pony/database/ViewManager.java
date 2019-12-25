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

import java.util.HashMap;

import com.pony.database.global.BlobAccessor;
import com.pony.database.jdbc.SQLQuery;
import com.pony.util.IntegerVector;

/**
 * A DatabaseConnection view manager.  This controls adding, updating, deleting,
 * and processing views inside the system view table.
 *
 * @author Tobias Downer
 */

public class ViewManager {

    /**
     * The DatabaseConnection.
     */
    private DatabaseConnection connection;

    /**
     * The context.
     */
    private DatabaseQueryContext context;

    /**
     * Set to true when the connection makes changes to the view table through
     * this manager.
     */
    private boolean view_table_changed;

    /**
     * A local cache of ViewDef objects mapped by row id in the system view
     * table.  This cache is invalidated when changes are committed to the system
     * view table.
     */
    private HashMap local_cache;

    /**
     * Constructs the ViewManager for a DatabaseConnection.
     */
    ViewManager(DatabaseConnection connection) {
        this.connection = connection;
        this.context = new DatabaseQueryContext(connection);
        this.local_cache = new HashMap();
        this.view_table_changed = false;

        // Attach a cache backed on the VIEW table which will invalidate the
        // connection cache whenever the view table is modified.
        connection.attachTableBackedCache(new TableBackedCache(Database.SYS_VIEW) {
            public void purgeCacheOfInvalidatedEntries(
                    IntegerVector added_rows, IntegerVector removed_rows) {
                // If there were changed then invalidate the cache
                if (view_table_changed) {
                    invalidateViewCache();
                    view_table_changed = false;
                }
                // Otherwise, if there were committed added or removed changes also
                // invalidate the cache,
                else if ((added_rows != null && added_rows.size() > 0) ||
                        (removed_rows != null && removed_rows.size() > 0)) {
                    invalidateViewCache();
                }
            }
        });

    }

    /**
     * Returns the local cache of ViewDef objects.  This cache is mapped from
     * row_id to view object.  The cache is invalidated when changes are
     * committed to the system view table.
     */
    private HashMap getViewCache() {
        return local_cache;
    }

    /**
     * Invalidates the view cache.
     */
    private void invalidateViewCache() {
        local_cache.clear();
    }

    /**
     * Given the SYS_VIEW table, this returns a new table that contains the
     * entry with the given view name, or an empty result if the view is not
     * found.
     * Generates an error if more than 1 entry found.
     */
    private Table findViewEntry(DataTable table,
                                TableName view_name) {

        Operator EQUALS = Operator.get("=");

        Variable schemav = table.getResolvedVariable(0);
        Variable namev = table.getResolvedVariable(1);

        Table t = table.simpleSelect(context, namev, EQUALS,
                new Expression(TObject.stringVal(view_name.getName())));
        t = t.exhaustiveSelect(context, Expression.simple(
                schemav, EQUALS, TObject.stringVal(view_name.getSchema())));

        // This should be at most 1 row in size
        if (t.getRowCount() > 1) {
            throw new RuntimeException(
                    "Assert failed: multiple view entries for " + view_name);
        }

        // Return the entries found.
        return t;

    }

    /**
     * Returns true if the view with the given name exists.
     */
    public boolean viewExists(TableName view_name) {

        DataTable table = connection.getTable(Database.SYS_VIEW);
        return findViewEntry(table, view_name).getRowCount() == 1;

    }

    /**
     * Defines a view.  If the view with the name has not been defined it is
     * defined.  If the view has been defined then it is overwritten with this
     * information.
     *
     * @param view information that defines the view.
     * @param query the query that forms the view.
     * @param user the user that owns this view being defined.
     */
    public void defineView(ViewDef view, SQLQuery query, User user)
            throws DatabaseException {

        DataTableDef data_table_def = view.getDataTableDef();
        DataTable view_table = connection.getTable(Database.SYS_VIEW);

        TableName view_name = data_table_def.getTableName();

        // Create the view record
        RowData rdat = new RowData(view_table);
        rdat.setColumnDataFromObject(0, data_table_def.getSchema());
        rdat.setColumnDataFromObject(1, data_table_def.getName());
        rdat.setColumnDataFromObject(2, query.serializeToBlob());
        rdat.setColumnDataFromObject(3, view.serializeToBlob());
        rdat.setColumnDataFromObject(4, user.getUserName());

        // Find the entry from the view that equals this name
        Table t = findViewEntry(view_table, view_name);

        // Delete the entry if it already exists.
        if (t.getRowCount() == 1) {
            view_table.delete(t);
        }

        // Insert the new view entry in the system view table
        view_table.add(rdat);

        // Notify that this database object has been successfully created.
        connection.databaseObjectCreated(view_name);

        // Change to the view table
        view_table_changed = true;

    }

    /**
     * Deletes the view with the given name, or returns false if no entries were
     * deleted from the view table.
     */
    public boolean deleteView(TableName view_name) throws DatabaseException {

        DataTable table = connection.getTable(Database.SYS_VIEW);

        // Find the entry from the view table that equal this name
        Table t = findViewEntry(table, view_name);

        // No entries so return false
        if (t.getRowCount() == 0) {
            return false;
        }

        table.delete(t);

        // Notify that this database object has been successfully dropped.
        connection.databaseObjectDropped(view_name);

        // Change to the view table
        view_table_changed = true;

        // Return that 1 or more entries were dropped.
        return true;
    }

    /**
     * Creates a ViewDef object for the given view name in the table.  The
     * access is cached through the given HashMap object.
     * <p>
     * We assume the access to the cache is limited to the current thread
     * calling this method.  We don't synchronize over the cache at any time.
     */
    private static ViewDef getViewDef(HashMap cache,
                                      TableDataSource view_table, TableName view_name) {

        RowEnumeration e = view_table.rowEnumeration();
        while (e.hasMoreRows()) {
            int row = e.nextRowIndex();

            String c_schema =
                    view_table.getCellContents(0, row).getObject().toString();
            String c_name =
                    view_table.getCellContents(1, row).getObject().toString();

            if (view_name.getSchema().equals(c_schema) &&
                    view_name.getName().equals(c_name)) {

                Object cache_key = new Long(row);
                ViewDef view_def = (ViewDef) cache.get(cache_key);

                if (view_def == null) {
                    // Not in the cache, so deserialize it and put it in the cache.
                    BlobAccessor blob =
                            (BlobAccessor) view_table.getCellContents(3, row).getObject();
                    // Derserialize the blob
                    view_def = ViewDef.deserializeFromBlob(blob);
                    // Put this in the cache....
                    cache.put(cache_key, view_def);

                }
                return view_def;
            }

        }

        throw new StatementException("View '" + view_name + "' not found.");

    }

    /**
     * Creates a ViewDef object for the given index value in the table.  The
     * access is cached through the given HashMap object.
     * <p>
     * We assume the access to the cache is limited to the current thread
     * calling this method.  We don't synchronize over the cache at any time.
     */
    private static ViewDef getViewDef(HashMap cache,
                                      TableDataSource view_table, int index) {

        RowEnumeration e = view_table.rowEnumeration();
        int i = 0;
        while (e.hasMoreRows()) {
            int row = e.nextRowIndex();

            if (i == index) {
                Object cache_key = new Long(row);
                ViewDef view_def = (ViewDef) cache.get(cache_key);

                if (view_def == null) {
                    // Not in the cache, so deserialize it and put it in the cache.
                    BlobAccessor blob =
                            (BlobAccessor) view_table.getCellContents(3, row).getObject();
                    // Derserialize the blob
                    view_def = ViewDef.deserializeFromBlob(blob);
                    // Put this in the cache....
                    cache.put(cache_key, view_def);

                }
                return view_def;
            }

            ++i;
        }
        throw new Error("Index out of range.");
    }

    /**
     * Returns a freshly deserialized QueryPlanNode object for the given view
     * object.
     */
    public QueryPlanNode createViewQueryPlanNode(TableName view_name) {
        DataTable table = connection.getTable(Database.SYS_VIEW);
        return getViewDef(local_cache, table, view_name).getQueryPlanNode();
    }

    /**
     * Returns an InternalTableInfo object used to model the list of views
     * that are accessible within the given Transaction object.  This is used to
     * model all views as regular tables accessible within a transaction.
     * <p>
     * Note that the 'ViewManager' parameter can be null if there is no backing
     * view manager.  The view manager is intended as a cache to improve the
     * access speed of the manager.
     */
    static InternalTableInfo createInternalTableInfo(ViewManager manager,
                                                     Transaction transaction) {
        return new ViewInternalTableInfo(manager, transaction);
    }

    // ---------- Inner classes ----------

    /**
     * An object that models the list of views as table objects in a
     * transaction.
     */
    private static class ViewInternalTableInfo
            extends AbstractInternalTableInfo2 {

        ViewManager view_manager;
        HashMap view_cache;

        ViewInternalTableInfo(ViewManager manager, Transaction transaction) {
            super(transaction, Database.SYS_VIEW);
            this.view_manager = manager;
            if (view_manager == null) {
                view_cache = new HashMap();
            } else {
                view_cache = view_manager.getViewCache();
            }
        }

        public String getTableType(int i) {
            return "VIEW";
        }

        public DataTableDef getDataTableDef(int i) {
            return getViewDef(view_cache,
                    transaction.getTable(Database.SYS_VIEW), i).getDataTableDef();
        }

        public MutableTableDataSource createInternalTable(int i) {
            throw new RuntimeException("Not supported for views.");
        }

    }

}

