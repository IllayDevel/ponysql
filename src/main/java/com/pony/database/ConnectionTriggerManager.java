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

import java.io.*;
import java.util.ArrayList;

import com.pony.util.IntegerVector;

/**
 * A trigger manager on a DatabaseConnection that maintains a list of all
 * triggers set in the database, and the types of triggers they are.  This
 * object is closely tied to a DatabaseConnection.
 * <p>
 * The trigger manager actually uses a trigger itself to maintain a list of
 * tables that have triggers, and the action to perform on the trigger.
 *
 * @author Tobias Downer
 */

public final class ConnectionTriggerManager {

    /**
     * The DatabaseConnection.
     */
    private final DatabaseConnection connection;

    /**
     * The list of triggers currently in view.
     * (TriggerInfo)
     */
    private final ArrayList triggers_active;

    /**
     * If this is false then the list is not validated and must be refreshed
     * when we next access trigger information.
     */
    private boolean list_validated;

    /**
     * True if the trigger table was modified during the last transaction.
     */
    private boolean trigger_modified;

    /**
     * Constructs the manager.
     */
    ConnectionTriggerManager(DatabaseConnection connection) {
        this.connection = connection;
        this.triggers_active = new ArrayList();
        this.list_validated = false;
        this.trigger_modified = false;
        // Attach a commit trigger listener
        connection.attachTableBackedCache(new CTMBackedCache());
    }

    /**
     * Returns a Table object that contains the trigger information with the
     * given name.  Returns an empty table if no trigger found.
     */
    private Table findTrigger(QueryContext context, DataTable table,
                              String schema, String name) {
        // Find all the trigger entries with this name
        Operator EQUALS = Operator.get("=");

        Variable schemav = table.getResolvedVariable(0);
        Variable namev = table.getResolvedVariable(1);

        Table t = table.simpleSelect(context, namev, EQUALS,
                new Expression(TObject.stringVal(name)));
        return t.exhaustiveSelect(context, Expression.simple(
                schemav, EQUALS, TObject.stringVal(schema)));
    }

    /**
     * Creates a new trigger action on a stored procedure and makes the change
     * to the transaction of this DatabaseConnection.  If the connection is
     * committed then the trigger is made a perminant change to the database.
     *
     * @param schema the schema name of the trigger.
     * @param name the name of the trigger.
     * @param type the type of trigger.
     * @param procedure_name the name of the procedure to execute.
     * @param params any constant parameters for the triggering procedure.
     */
    public void createTableTrigger(String schema, String name,
                                   int type, TableName on_table,
                                   String procedure_name, TObject[] params)
            throws DatabaseException {

        TableName trigger_table_name = new TableName(schema, name);

        // Check this name is not reserved
        DatabaseConnection.checkAllowCreate(trigger_table_name);

        // Before adding the trigger, make sure this name doesn't already resolve
        // to an object in the database with this schema/name.
        if (!connection.tableExists(trigger_table_name)) {

            // Encode the parameters
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            try {
                ObjectOutputStream ob_out = new ObjectOutputStream(bout);
                ob_out.writeInt(1); // version
                ob_out.writeObject(params);
                ob_out.flush();
            } catch (IOException e) {
                throw new RuntimeException("IO Error: " + e.getMessage());
            }
            byte[] encoded_params = bout.toByteArray();

            // Insert the entry into the trigger table,
            DataTable table = connection.getTable(Database.SYS_DATA_TRIGGER);
            RowData row = new RowData(table);
            row.setColumnDataFromTObject(0, TObject.stringVal(schema));
            row.setColumnDataFromTObject(1, TObject.stringVal(name));
            row.setColumnDataFromTObject(2, TObject.intVal(type));
            row.setColumnDataFromTObject(3,
                    TObject.stringVal("T:" + on_table.toString()));
            row.setColumnDataFromTObject(4, TObject.stringVal(procedure_name));
            row.setColumnDataFromTObject(5, TObject.objectVal(encoded_params));
            row.setColumnDataFromTObject(6,
                    TObject.stringVal(connection.getUser().getUserName()));
            table.add(row);

            // Invalidate the list
            invalidateTriggerList();

            // Notify that this database object has been successfully created.
            connection.databaseObjectCreated(trigger_table_name);

            // Flag that this transaction modified the trigger table.
            trigger_modified = true;
        } else {
            throw new RuntimeException("Trigger name '" + schema + "." + name +
                    "' already in use.");
        }
    }

    /**
     * Drops a trigger that has previously been defined.
     */
    public void dropTrigger(String schema, String name) throws DatabaseException {
        QueryContext context = new DatabaseQueryContext(connection);
        DataTable table = connection.getTable(Database.SYS_DATA_TRIGGER);

        // Find the trigger
        Table t = findTrigger(context, table, schema, name);

        if (t.getRowCount() == 0) {
            throw new StatementException("Trigger '" + schema + "." + name +
                    "' not found.");
        } else if (t.getRowCount() > 1) {
            throw new RuntimeException(
                    "Assertion failed: multiple entries for the same trigger name.");
        } else {
            // Drop this trigger,
            table.delete(t);

            // Notify that this database object has been successfully dropped.
            connection.databaseObjectDropped(new TableName(schema, name));

            // Flag that this transaction modified the trigger table.
            trigger_modified = true;
        }

    }

    /**
     * Returns true if the trigger exists, false otherwise.
     */
    public boolean triggerExists(String schema, String name) {
        QueryContext context = new DatabaseQueryContext(connection);
        DataTable table = connection.getTable(Database.SYS_DATA_TRIGGER);

        // Find the trigger
        Table t = findTrigger(context, table, schema, name);

        if (t.getRowCount() == 0) {
            // Trigger wasn't found
            return false;
        } else if (t.getRowCount() > 1) {
            throw new RuntimeException(
                    "Assertion failed: multiple entries for the same trigger name.");
        } else {
            // Trigger found
            return true;
        }
    }

    /**
     * Invalidates the trigger list causing the list to rebuild when a potential
     * triggering event next occurs.
     * <p>
     * NOTE: must only be called from the thread that owns the
     *   DatabaseConnection.
     */
    private void invalidateTriggerList() {
        list_validated = false;
        triggers_active.clear();
    }

    /**
     * Build the trigger list if it is not validated.
     */
    private void buildTriggerList() {
        if (!list_validated) {
            // Cache the trigger table
            DataTable table = connection.getTable(Database.SYS_DATA_TRIGGER);
            RowEnumeration e = table.rowEnumeration();

            // For each row
            while (e.hasMoreRows()) {
                int row_index = e.nextRowIndex();

                TObject trig_schem = table.getCellContents(0, row_index);
                TObject trig_name = table.getCellContents(1, row_index);
                TObject type = table.getCellContents(2, row_index);
                TObject on_object = table.getCellContents(3, row_index);
                TObject action = table.getCellContents(4, row_index);
                TObject misc = table.getCellContents(5, row_index);

                TriggerInfo trigger_info = new TriggerInfo();
                trigger_info.schema = trig_schem.getObject().toString();
                trigger_info.name = trig_name.getObject().toString();
                trigger_info.type = type.toBigNumber().intValue();
                trigger_info.on_object = on_object.getObject().toString();
                trigger_info.action = action.getObject().toString();
                trigger_info.misc = misc;

                // Add to the list
                triggers_active.add(trigger_info);
            }

            list_validated = true;
        }
    }

    /**
     * Performs any trigger action for this event.  For example, if we have it
     * setup so a trigger fires when there is an INSERT event on table x then
     * we perform the triggering procedure right here.
     */
    void performTriggerAction(TableModificationEvent evt) {
        // REINFORCED NOTE: The 'tableExists' call is REALLY important.  First it
        //   makes sure the transaction on the connection is established (it should
        //   be anyway if a trigger is firing), and it also makes sure the trigger
        //   table exists - which it may not be during database init.
        if (connection.tableExists(Database.SYS_DATA_TRIGGER)) {
            // If the trigger list isn't built, then do so now
            buildTriggerList();

            // On object value to test for,
            TableName table_name = evt.getTableName();
            String on_ob_test = "T:" + table_name.toString();

            // Search the triggers list for an event that matches this event
            int sz = triggers_active.size();
            for (int i = 0; i < sz; ++i) {
                TriggerInfo t_info = (TriggerInfo) triggers_active.get(i);
                if (t_info.on_object.equals(on_ob_test)) {
                    // Table name matches
                    // Do the types match?  eg. before/after match, and
                    // insert/delete/update is being listened to.
                    if (evt.listenedBy(t_info.type)) {
                        // Type matches this trigger, so we need to fire it
                        // Parse the action string
                        String action = t_info.action;
                        // Get the procedure name to fire (qualify it against the schema
                        // of the table being fired).
                        ProcedureName procedure_name =
                                ProcedureName.qualify(table_name.getSchema(), action);
                        // Set up OLD and NEW tables

                        // Record the old table state
                        DatabaseConnection.OldNewTableState current_state =
                                connection.getOldNewTableState();

                        // Set the new table state
                        // If an INSERT event then we setup NEW to be the row being inserted
                        // If an DELETE event then we setup OLD to be the row being deleted
                        // If an UPDATE event then we setup NEW to be the row after the
                        // update, and OLD to be the row before the update.
                        connection.setOldNewTableState(
                                new DatabaseConnection.OldNewTableState(table_name,
                                        evt.getRowIndex(), evt.getRowData(), evt.isBefore()));

                        try {
                            // Invoke the procedure (no arguments)
                            connection.getProcedureManager().invokeProcedure(
                                    procedure_name, new TObject[0]);
                        } finally {
                            // Reset the OLD and NEW tables to previous values
                            connection.setOldNewTableState(current_state);
                        }

                    }

                }

            }  // for each trigger

        }

    }

    /**
     * Returns an InternalTableInfo object used to model the list of triggers
     * that are accessible within the given Transaction object.  This is used to
     * model all triggers that have been defined as tables.
     */
    static InternalTableInfo createInternalTableInfo(Transaction transaction) {
        return new TriggerInternalTableInfo(transaction);
    }

    // ---------- Inner classes ----------

    /**
     * A TableBackedCache that manages the list of connection level triggers that
     * are currently active on this connection.
     */
    private class CTMBackedCache extends TableBackedCache {

        /**
         * Constructor.
         */
        public CTMBackedCache() {
            super(Database.SYS_DATA_TRIGGER);
        }

        public void purgeCacheOfInvalidatedEntries(
                IntegerVector added_rows, IntegerVector removed_rows) {
            // Note that this is called when a transaction is started or stopped.

            // If the trigger table was modified, we need to invalidate the trigger
            // list.  This covers the case when we rollback a trigger table change
            if (trigger_modified) {
                invalidateTriggerList();
                trigger_modified = false;
            }
            // If any data has been committed removed then completely flush the
            // cache.
            else if ((removed_rows != null && removed_rows.size() > 0) ||
                    (added_rows != null && added_rows.size() > 0)) {
                invalidateTriggerList();
            }
        }

    }

    /**
     * Container class for all trigger actions defined on the database.
     */
    private static class TriggerInfo {
        String schema;
        String name;
        int type;
        String on_object;
        String action;
        TObject misc;
    }

    /**
     * An object that models the list of triggers as table objects in a
     * transaction.
     */
    private static class TriggerInternalTableInfo
            extends AbstractInternalTableInfo2 {

        TriggerInternalTableInfo(Transaction transaction) {
            super(transaction, Database.SYS_DATA_TRIGGER);
        }

        private static DataTableDef createDataTableDef(String schema, String name) {
            // Create the DataTableDef that describes this entry
            DataTableDef def = new DataTableDef();
            def.setTableName(new TableName(schema, name));

            // Add column definitions
            def.addColumn(DataTableColumnDef.createNumericColumn("type"));
            def.addColumn(DataTableColumnDef.createStringColumn("on_object"));
            def.addColumn(DataTableColumnDef.createStringColumn("procedure_name"));
            def.addColumn(DataTableColumnDef.createStringColumn("param_args"));
            def.addColumn(DataTableColumnDef.createStringColumn("owner"));

            // Set to immutable
            def.setImmutable();

            // Return the data table def
            return def;
        }


        public String getTableType(int i) {
            return "TRIGGER";
        }

        public DataTableDef getDataTableDef(int i) {
            TableName table_name = getTableName(i);
            return createDataTableDef(table_name.getSchema(), table_name.getName());
        }

        public MutableTableDataSource createInternalTable(int index) {
            MutableTableDataSource table =
                    transaction.getTable(Database.SYS_DATA_TRIGGER);
            RowEnumeration row_e = table.rowEnumeration();
            int p = 0;
            int i;
            int row_i = -1;
            while (row_e.hasMoreRows()) {
                i = row_e.nextRowIndex();
                if (p == index) {
                    row_i = i;
                } else {
                    ++p;
                }
            }
            if (p == index) {
                String schema = table.getCellContents(0, row_i).getObject().toString();
                String name = table.getCellContents(1, row_i).getObject().toString();

                final DataTableDef table_def = createDataTableDef(schema, name);
                final TObject type = table.getCellContents(2, row_i);
                final TObject on_object = table.getCellContents(3, row_i);
                final TObject procedure_name = table.getCellContents(4, row_i);
                final TObject param_args = table.getCellContents(5, row_i);
                final TObject owner = table.getCellContents(6, row_i);

                // Implementation of MutableTableDataSource that describes this
                // trigger.
                return new GTDataSource(transaction.getSystem()) {
                    public DataTableDef getDataTableDef() {
                        return table_def;
                    }

                    public int getRowCount() {
                        return 1;
                    }

                    public TObject getCellContents(int col, int row) {
                        switch (col) {
                            case 0:
                                return type;
                            case 1:
                                return on_object;
                            case 2:
                                return procedure_name;
                            case 3:
                                return param_args;
                            case 4:
                                return owner;
                            default:
                                throw new RuntimeException("Column out of bounds.");
                        }
                    }
                };

            } else {
                throw new RuntimeException("Index out of bounds.");
            }

        }

    }

}

