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
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.pony.util.IntegerListInterface;
import com.pony.util.IntegerIterator;
import com.pony.util.IntegerVector;
import com.pony.util.ByteArrayUtil;
import com.pony.util.UserTerminal;
import com.pony.util.BigNumber;
import com.pony.debug.*;

import com.pony.store.Store;
import com.pony.store.MutableArea;
import com.pony.store.Area;

import com.pony.database.StateStore.StateResource;

import com.pony.database.global.ByteLongObject;
import com.pony.database.global.ObjectTranslator;
import com.pony.database.global.Ref;

/**
 * A conglomerate of data that represents the contents of all tables in a
 * complete database.  This object handles all data persistance management
 * (storage, retrieval, removal) issues.  It is a transactional manager for
 * both data and indices in the database.
 *
 * @author Tobias Downer
 */

public class TableDataConglomerate {

    /**
     * The postfix on the name of the state file for the database store name.
     */
    public static final String STATE_POST = "_sf";

    // ---------- The standard constraint/schema tables ----------

    /**
     * The name of the system schema where persistant conglomerate state is
     * stored.
     */
    public static final String SYSTEM_SCHEMA = "SYS_INFO";

    /**
     * The schema info table.
     */
    public static final TableName SCHEMA_INFO_TABLE =
            new TableName(SYSTEM_SCHEMA, "SchemaInfo");

    public static final TableName PERSISTENT_VAR_TABLE =
            new TableName(SYSTEM_SCHEMA, "DatabaseVars");

    public static final TableName FOREIGN_COLS_TABLE =
            new TableName(SYSTEM_SCHEMA, "ForeignColumns");

    public static final TableName UNIQUE_COLS_TABLE =
            new TableName(SYSTEM_SCHEMA, "UniqueColumns");

    public static final TableName PRIMARY_COLS_TABLE =
            new TableName(SYSTEM_SCHEMA, "PrimaryColumns");

    public static final TableName CHECK_INFO_TABLE =
            new TableName(SYSTEM_SCHEMA, "CheckInfo");

    public static final TableName UNIQUE_INFO_TABLE =
            new TableName(SYSTEM_SCHEMA, "UniqueInfo");

    public static final TableName FOREIGN_INFO_TABLE =
            new TableName(SYSTEM_SCHEMA, "FKeyInfo");

    public static final TableName PRIMARY_INFO_TABLE =
            new TableName(SYSTEM_SCHEMA, "PKeyInfo");

    public static final TableName SYS_SEQUENCE_INFO =
            new TableName(SYSTEM_SCHEMA, "SequenceInfo");

    public static final TableName SYS_SEQUENCE =
            new TableName(SYSTEM_SCHEMA, "Sequence");

    /**
     * The TransactionSystem that this Conglomerate is a child of.
     */
    private final TransactionSystem system;

    /**
     * The StoreSystem object used by this conglomerate to store the underlying
     * representation.
     */
    private final StoreSystem store_system;

    /**
     * The name given to this conglomerate.
     */
    private String name;

    /**
     * The actual store that backs the state store.
     */
    private Store act_state_store;

    /**
     * A store for the conglomerate state container.  This
     * file stores information persistantly about the state of this object.
     */
    private StateStore state_store;

    /**
     * The current commit id for committed transactions.  Whenever transactional
     * changes are committed to the conglomerate, this id is incremented.
     */
    private long commit_id;


    /**
     * The list of all tables that are currently open in this conglomerate.
     * This includes tables that are not committed.
     */
    private ArrayList table_list;

    /**
     * The actual Store implementation that maintains the BlobStore information
     * for this conglomerate (if there is one).
     */
    private Store act_blob_store;

    /**
     * The BlobStore object for this conglomerate.
     */
    private BlobStore blob_store;

    /**
     * The SequenceManager object for this conglomerate.
     */
    private SequenceManager sequence_manager;

    /**
     * The list of transactions that are currently open over this conglomerate.
     * This list is ordered from lowest commit_id to highest.  This object is
     * shared with all the children MasterTableDataSource objects.
     */
    private OpenTransactionList open_transactions;

    /**
     * The list of all name space journals for the history of committed
     * transactions.
     */
    private ArrayList namespace_journal_list;

    // ---------- Table event listener ----------

    /**
     * All listeners for modification events on tables in this conglomerate.
     * This is a mapping from TableName -> ArrayList of listeners.
     */
    private final HashMap modification_listeners;


    // ---------- Locks ----------

    /**
     * This lock is obtained when we go to commit a change to the table.
     * Grabbing this lock ensures that no other commits can occur at the same
     * time on this conglomerate.
     */
    final Object commit_lock = new Object();

//  // ---------- Shutdown hook thread ----------
//  
//  /**
//   * The ConglomerateShutdownHookThread object which we create when the
//   * conglomerate in openned, and removed when we close the conglomerate.
//   */
//  private ConglomerateShutdownHookThread shutdown_hook = null;


    /**
     * Constructs the conglomerate.
     */
    public TableDataConglomerate(TransactionSystem system,
                                 StoreSystem store_system) {
        this.system = system;
        this.store_system = store_system;
        this.open_transactions = new OpenTransactionList(system);
        this.modification_listeners = new HashMap();
        this.namespace_journal_list = new ArrayList();

        this.sequence_manager = new SequenceManager(this);

    }

    /**
     * Returns the TransactionSystem that this conglomerate is part of.
     */
    public final TransactionSystem getSystem() {
        return system;
    }

    /**
     * Returns the StoreSystem used by this conglomerate to manage the
     * persistent state of the database.
     */
    public final StoreSystem storeSystem() {
        return store_system;
    }

    /**
     * Returns the SequenceManager object for this conglomerate.
     */
    final SequenceManager getSequenceManager() {
        return sequence_manager;
    }

    /**
     * Returns the BlobStore for this conglomerate.
     */
    final BlobStore getBlobStore() {
        return blob_store;
    }

    /**
     * Returns the DebugLogger object that we use to log debug messages to.
     */
    public final DebugLogger Debug() {
        return getSystem().Debug();
    }

    /**
     * Returns the name given to this conglomerate.
     */
    String getName() {
        return name;
    }

    // ---------- Conglomerate state methods ----------

    /**
     * Marks the given table id as committed dropped.
     */
    private void markAsCommittedDropped(int table_id) {
        MasterTableDataSource master_table = getMasterTable(table_id);
        state_store.addDeleteResource(
                new StateResource(table_id, createEncodedTableFile(master_table)));
    }

    /**
     * Loads the master table given the table_id and the name of the table
     * resource in the database path.  The table_string is a specially formatted
     * string that we parse to determine the file structure of the table.
     */
    private MasterTableDataSource loadMasterTable(int table_id,
                                                  String table_str, int table_type) throws IOException {

        // Open the table
        if (table_type == 1) {
            V1MasterTableDataSource master =
                    new V1MasterTableDataSource(getSystem(),
                            storeSystem(), open_transactions);
            if (master.exists(table_str)) {
                return master;
            }
        } else if (table_type == 2) {
            V2MasterTableDataSource master =
                    new V2MasterTableDataSource(getSystem(),
                            storeSystem(), open_transactions, blob_store);
            if (master.exists(table_str)) {
                return master;
            }
        }

        // If not exists, then generate an error message
        Debug().write(Lvl.ERROR, this,
                "Couldn't find table source - resource name: " +
                        table_str + " table_id: " + table_id);

        return null;
    }

    /**
     * Returns a string that is an encoded table file name.  An encoded table
     * file name includes information about the table type with the name of the
     * table.  For example, ":1ThisTable" represents a V1MasterTableDataSource
     * table with file name "ThisTable".
     */
    private static String createEncodedTableFile(MasterTableDataSource table) {
        char type;
        if (table instanceof V1MasterTableDataSource) {
            type = '1';
        } else if (table instanceof V2MasterTableDataSource) {
            type = '2';
        } else {
            throw new RuntimeException("Unrecognised MasterTableDataSource class.");
        }
        StringBuffer buf = new StringBuffer();
        buf.append(':');
        buf.append(type);
        buf.append(table.getSourceIdent());
        return new String(buf);
    }

    /**
     * Reads in the list of committed tables in this conglomerate.  This should
     * only be called during an 'open' like method.  This method fills the
     * 'committed_tables' and 'table_list' lists with the tables in this
     * conglomerate.
     */
    private void readVisibleTables() throws IOException {

        // The list of all visible tables from the state file
        StateResource[] tables = state_store.getVisibleList();
        // For each visible table
        for (int i = 0; i < tables.length; ++i) {
            StateResource resource = tables[i];

            int master_table_id = (int) resource.table_id;
            String file_name = resource.name;

            // Parse the file name string and determine the table type.
            int table_type = 1;
            if (file_name.startsWith(":")) {
                if (file_name.charAt(1) == '1') {
                    table_type = 1;
                } else if (file_name.charAt(1) == '2') {
                    table_type = 2;
                } else {
                    throw new RuntimeException("Table type is not known.");
                }
                file_name = file_name.substring(2);
            }

            // Load the master table from the resource information
            MasterTableDataSource master =
                    loadMasterTable(master_table_id, file_name, table_type);

            if (master == null) {
                throw new Error("Table file for " + file_name + " was not found.");
            }

            if (master instanceof V1MasterTableDataSource) {
                V1MasterTableDataSource v1_master = (V1MasterTableDataSource) master;
                v1_master.open(file_name);
            } else if (master instanceof V2MasterTableDataSource) {
                V2MasterTableDataSource v2_master = (V2MasterTableDataSource) master;
                v2_master.open(file_name);
            } else {
                throw new Error("Unknown master table type: " + master.getClass());
            }

            // Add the table to the table list
            table_list.add(master);

        }

    }

    /**
     * Checks the list of committed tables in this conglomerate.  This should
     * only be called during an 'check' like method.  This method fills the
     * 'committed_tables' and 'table_list' lists with the tables in this
     * conglomerate.
     */
    public void checkVisibleTables(UserTerminal terminal) throws IOException {

        // The list of all visible tables from the state file
        StateResource[] tables = state_store.getVisibleList();
        // For each visible table
        for (int i = 0; i < tables.length; ++i) {
            StateResource resource = tables[i];

            int master_table_id = (int) resource.table_id;
            String file_name = resource.name;

            // Parse the file name string and determine the table type.
            int table_type = 1;
            if (file_name.startsWith(":")) {
                if (file_name.charAt(1) == '1') {
                    table_type = 1;
                } else if (file_name.charAt(1) == '2') {
                    table_type = 2;
                } else {
                    throw new RuntimeException("Table type is not known.");
                }
                file_name = file_name.substring(2);
            }

            // Load the master table from the resource information
            MasterTableDataSource master =
                    loadMasterTable(master_table_id, file_name, table_type);

            if (master instanceof V1MasterTableDataSource) {
                V1MasterTableDataSource v1_master = (V1MasterTableDataSource) master;
                v1_master.checkAndRepair(file_name, terminal);
            } else if (master instanceof V2MasterTableDataSource) {
                V2MasterTableDataSource v2_master = (V2MasterTableDataSource) master;
                v2_master.checkAndRepair(file_name, terminal);
            } else {
                throw new Error("Unknown master table type: " + master.getClass());
            }

            // Add the table to the table list
            table_list.add(master);

            // Set a check point
            store_system.setCheckPoint();

        }

    }


    /**
     * Reads in the list of committed dropped tables on this conglomerate.  This
     * should only be called during an 'open' like method.  This method fills
     * the 'committed_dropped' and 'table_list' lists with the tables in this
     * conglomerate.
     * <p>
     * @param terminal the terminal to ask questions if problems are found.  If
     *   null then an exception is thrown if there are problems.
     */
    private void readDroppedTables() throws IOException {

        // The list of all dropped tables from the state file
        StateResource[] tables = state_store.getDeleteList();
        // For each visible table
        for (int i = 0; i < tables.length; ++i) {
            StateResource resource = tables[i];

            int master_table_id = (int) resource.table_id;
            String file_name = resource.name;

            // Parse the file name string and determine the table type.
            int table_type = 1;
            if (file_name.startsWith(":")) {
                if (file_name.charAt(1) == '1') {
                    table_type = 1;
                } else if (file_name.charAt(1) == '2') {
                    table_type = 2;
                } else {
                    throw new RuntimeException("Table type is not known.");
                }
                file_name = file_name.substring(2);
            }

            // Load the master table from the resource information
            MasterTableDataSource master =
                    loadMasterTable(master_table_id, file_name, table_type);

            // File wasn't found so remove from the delete resources
            if (master == null) {
                state_store.removeDeleteResource(resource.name);
            } else {
                if (master instanceof V1MasterTableDataSource) {
                    V1MasterTableDataSource v1_master = (V1MasterTableDataSource) master;
                    v1_master.open(file_name);
                } else if (master instanceof V2MasterTableDataSource) {
                    V2MasterTableDataSource v2_master = (V2MasterTableDataSource) master;
                    v2_master.open(file_name);
                } else {
                    throw new Error("Unknown master table type: " + master.getClass());
                }

                // Add the table to the table list
                table_list.add(master);
            }

        }

        // Commit any changes to the state store
        state_store.commit();

    }

    /**
     * Create the system tables that must be present in a conglomerates.  These
     * tables consist of contraint and table management data.
     * <p>
     * <pre>
     * PKeyInfo - Primary key constraint information.
     * FKeyInfo - Foreign key constraint information.
     * UniqueInfo - Unique set constraint information.
     * CheckInfo  - Check constraint information.
     * PrimaryColumns - Primary columns information (refers to PKeyInfo)
     * UniqueColumns  - Unique columns information (refers to UniqueInfo)
     * ForeignColumns1 - Foreign column information (refers to FKeyInfo)
     * ForeignColumns2 - Secondary Foreign column information (refers to
     *                       FKeyInfo).
     * </pre>
     * These tables handle data for referential integrity.  There are also some
     * additional tables containing general table information.
     * <pre>
     * TableColumnInfo - All table and column information.
     * </pre>
     * The design is fairly elegant in that we are using the database to store
     * information to maintain referential integrity.
     * <p><pre>
     * The schema layout for these tables;
     *
     *  CREATE TABLE PKeyInfo (
     *    id          NUMERIC NOT NULL,
     *    name        TEXT NOT NULL,  // The name of the primary key constraint
     *    schema      TEXT NOT NULL,  // The name of the schema
     *    table       TEXT NOT NULL,  // The name of the table
     *    deferred    BIT  NOT NULL,  // Whether deferred or immediate
     *    PRIMARY KEY (id),
     *    UNIQUE (schema, table)
     *  );
     *  CREATE TABLE FKeyInfo (
     *    id          NUMERIC NOT NULL,
     *    name        TEXT NOT NULL,  // The name of the foreign key constraint
     *    schema      TEXT NOT NULL,  // The name of the schema
     *    table       TEXT NOT NULL,  // The name of the table
     *    ref_schema  TEXT NOT NULL,  // The name of the schema referenced
     *    ref_table   TEXT NOT NULL,  // The name of the table referenced
     *    update_rule TEXT NOT NULL,  // The rule for updating to table
     *    delete_rule TEXT NOT NULL,  // The rule for deleting from table
     *    deferred    BIT  NOT NULL,  // Whether deferred or immediate
     *    PRIMARY KEY (id)
     *  );
     *  CREATE TABLE UniqueInfo (
     *    id          NUMERIC NOT NULL,
     *    name        TEXT NOT NULL,  // The name of the unique constraint
     *    schema      TEXT NOT NULL,  // The name of the schema
     *    table       TEXT NOT NULL,  // The name of the table
     *    deferred    BIT  NOT NULL,  // Whether deferred or immediate
     *    PRIMARY KEY (id)
     *  );
     *  CREATE TABLE CheckInfo (
     *    id          NUMERIC NOT NULL,
     *    name        TEXT NOT NULL,  // The name of the check constraint
     *    schema      TEXT NOT NULL,  // The name of the schema
     *    table       TEXT NOT NULL,  // The name of the table
     *    expression  TEXT NOT NULL,  // The check expression
     *    deferred    BIT  NOT NULL,  // Whether deferred or immediate
     *    PRIMARY KEY (id)
     *  );
     *  CREATE TABLE PrimaryColumns (
     *    pk_id   NUMERIC NOT NULL, // The primary key constraint id
     *    column  TEXT NOT NULL,    // The name of the primary
     *    seq_no  INTEGER NOT NULL, // The sequence number of this constraint
     *    FOREIGN KEY pk_id REFERENCES PKeyInfo
     *  );
     *  CREATE TABLE UniqueColumns (
     *    un_id   NUMERIC NOT NULL, // The unique constraint id
     *    column  TEXT NOT NULL,    // The column that is unique
     *    seq_no  INTEGER NOT NULL, // The sequence number of this constraint
     *    FOREIGN KEY un_id REFERENCES UniqueInfo
     *  );
     *  CREATE TABLE ForeignColumns (
     *    fk_id   NUMERIC NOT NULL, // The foreign key constraint id
     *    fcolumn TEXT NOT NULL,    // The column in the foreign key
     *    pcolumn TEXT NOT NULL,    // The column in the primary key
     *                              // (referenced)
     *    seq_no  INTEGER NOT NULL, // The sequence number of this constraint
     *    FOREIGN KEY fk_id REFERENCES FKeyInfo
     *  );
     *  CREATE TABLE SchemaInfo (
     *    id     NUMERIC NOT NULL,
     *    name   TEXT NOT NULL,
     *    type   TEXT,              // Schema type (system, etc)
     *    other  TEXT,
     *
     *    UNIQUE ( name )
     *  );
     *  CREATE TABLE TableInfo (
     *    id     NUMERIC NOT NULL,
     *    name   TEXT NOT NULL,     // The name of the table
     *    schema TEXT NOT NULL,     // The name of the schema of this table
     *    type   TEXT,              // Table type (temporary, system, etc)
     *    other  TEXT,              // Notes, etc
     *
     *    UNIQUE ( name )
     *  );
     *  CREATE TABLE ColumnColumns (
     *    t_id    NUMERIC NOT NULL,  // Foreign key to TableInfo
     *    column  TEXT NOT NULL,     // The column name
     *    seq_no  INTEGER NOT NULL,  // The sequence in the table
     *    type    TEXT NOT NULL,     // The SQL type of this column
     *    size    NUMERIC,           // The size of the column if applicable
     *    scale   NUMERIC,           // The scale of the column if applicable
     *    default TEXT NOT NULL,     // The default expression
     *    constraints TEXT NOT NULL, // The constraints of this column
     *    other   TEXT,              // Notes, etc
     *
     *    FOREIGN KEY t_id REFERENCES TableInfo,
     *    UNIQUE ( t_id, column )
     *  );
     *
     * </pre>
     */
    void updateSystemTableSchema() {
        // Create the transaction
        Transaction transaction = createTransaction();

        DataTableDef table;

        table = new DataTableDef();
        table.setTableName(SYS_SEQUENCE_INFO);
        table.addColumn(DataTableColumnDef.createNumericColumn("id"));
        table.addColumn(DataTableColumnDef.createStringColumn("schema"));
        table.addColumn(DataTableColumnDef.createStringColumn("name"));
        table.addColumn(DataTableColumnDef.createNumericColumn("type"));
        transaction.alterCreateTable(table, 187, 128);

        table = new DataTableDef();
        table.setTableName(SYS_SEQUENCE);
        table.addColumn(DataTableColumnDef.createNumericColumn("seq_id"));
        table.addColumn(DataTableColumnDef.createNumericColumn("last_value"));
        table.addColumn(DataTableColumnDef.createNumericColumn("increment"));
        table.addColumn(DataTableColumnDef.createNumericColumn("minvalue"));
        table.addColumn(DataTableColumnDef.createNumericColumn("maxvalue"));
        table.addColumn(DataTableColumnDef.createNumericColumn("start"));
        table.addColumn(DataTableColumnDef.createNumericColumn("cache"));
        table.addColumn(DataTableColumnDef.createBooleanColumn("cycle"));
        transaction.alterCreateTable(table, 187, 128);

        table = new DataTableDef();
        table.setTableName(PRIMARY_INFO_TABLE);
        table.addColumn(DataTableColumnDef.createNumericColumn("id"));
        table.addColumn(DataTableColumnDef.createStringColumn("name"));
        table.addColumn(DataTableColumnDef.createStringColumn("schema"));
        table.addColumn(DataTableColumnDef.createStringColumn("table"));
        table.addColumn(DataTableColumnDef.createNumericColumn("deferred"));
        transaction.alterCreateTable(table, 187, 128);

        table = new DataTableDef();
        table.setTableName(FOREIGN_INFO_TABLE);
        table.addColumn(DataTableColumnDef.createNumericColumn("id"));
        table.addColumn(DataTableColumnDef.createStringColumn("name"));
        table.addColumn(DataTableColumnDef.createStringColumn("schema"));
        table.addColumn(DataTableColumnDef.createStringColumn("table"));
        table.addColumn(DataTableColumnDef.createStringColumn("ref_schema"));
        table.addColumn(DataTableColumnDef.createStringColumn("ref_table"));
        table.addColumn(DataTableColumnDef.createStringColumn("update_rule"));
        table.addColumn(DataTableColumnDef.createStringColumn("delete_rule"));
        table.addColumn(DataTableColumnDef.createNumericColumn("deferred"));
        transaction.alterCreateTable(table, 187, 128);

        table = new DataTableDef();
        table.setTableName(UNIQUE_INFO_TABLE);
        table.addColumn(DataTableColumnDef.createNumericColumn("id"));
        table.addColumn(DataTableColumnDef.createStringColumn("name"));
        table.addColumn(DataTableColumnDef.createStringColumn("schema"));
        table.addColumn(DataTableColumnDef.createStringColumn("table"));
        table.addColumn(DataTableColumnDef.createNumericColumn("deferred"));
        transaction.alterCreateTable(table, 187, 128);

        table = new DataTableDef();
        table.setTableName(CHECK_INFO_TABLE);
        table.addColumn(DataTableColumnDef.createNumericColumn("id"));
        table.addColumn(DataTableColumnDef.createStringColumn("name"));
        table.addColumn(DataTableColumnDef.createStringColumn("schema"));
        table.addColumn(DataTableColumnDef.createStringColumn("table"));
        table.addColumn(DataTableColumnDef.createStringColumn("expression"));
        table.addColumn(DataTableColumnDef.createNumericColumn("deferred"));
        table.addColumn(
                DataTableColumnDef.createBinaryColumn("serialized_expression"));
        transaction.alterCreateTable(table, 187, 128);

        table = new DataTableDef();
        table.setTableName(PRIMARY_COLS_TABLE);
        table.addColumn(DataTableColumnDef.createNumericColumn("pk_id"));
        table.addColumn(DataTableColumnDef.createStringColumn("column"));
        table.addColumn(DataTableColumnDef.createNumericColumn("seq_no"));
        transaction.alterCreateTable(table, 91, 128);

        table = new DataTableDef();
        table.setTableName(UNIQUE_COLS_TABLE);
        table.addColumn(DataTableColumnDef.createNumericColumn("un_id"));
        table.addColumn(DataTableColumnDef.createStringColumn("column"));
        table.addColumn(DataTableColumnDef.createNumericColumn("seq_no"));
        transaction.alterCreateTable(table, 91, 128);

        table = new DataTableDef();
        table.setTableName(FOREIGN_COLS_TABLE);
        table.addColumn(DataTableColumnDef.createNumericColumn("fk_id"));
        table.addColumn(DataTableColumnDef.createStringColumn("fcolumn"));
        table.addColumn(DataTableColumnDef.createStringColumn("pcolumn"));
        table.addColumn(DataTableColumnDef.createNumericColumn("seq_no"));
        transaction.alterCreateTable(table, 91, 128);

        table = new DataTableDef();
        table.setTableName(SCHEMA_INFO_TABLE);
        table.addColumn(DataTableColumnDef.createNumericColumn("id"));
        table.addColumn(DataTableColumnDef.createStringColumn("name"));
        table.addColumn(DataTableColumnDef.createStringColumn("type"));
        table.addColumn(DataTableColumnDef.createStringColumn("other"));
        transaction.alterCreateTable(table, 91, 128);

        // Stores misc variables of the database,
        table = new DataTableDef();
        table.setTableName(PERSISTENT_VAR_TABLE);
        table.addColumn(DataTableColumnDef.createStringColumn("variable"));
        table.addColumn(DataTableColumnDef.createStringColumn("value"));
        transaction.alterCreateTable(table, 91, 128);

        // Commit and close the transaction.
        try {
            transaction.closeAndCommit();
        } catch (TransactionException e) {
            Debug().writeException(e);
            throw new Error("Transaction Exception creating conglomerate.");
        }

    }

    /**
     * Given a table with a 'id' field, this will check that the sequence
     * value for the table is at least greater than the maximum id in the column.
     */
    void resetTableID(TableName tname) {
        // Create the transaction
        Transaction transaction = createTransaction();
        // Get the table
        MutableTableDataSource table = transaction.getTable(tname);
        // Find the index of the column name called 'id'
        DataTableDef table_def = table.getDataTableDef();
        int col_index = table_def.findColumnName("id");
        if (col_index == -1) {
            throw new Error("Column name 'id' not found.");
        }
        // Find the maximum 'id' value.
        SelectableScheme scheme = table.getColumnScheme(col_index);
        IntegerVector ivec = scheme.selectLast();
        if (ivec.size() > 0) {
            TObject ob = table.getCellContents(col_index, ivec.intAt(0));
            BigNumber b_num = ob.toBigNumber();
            if (b_num != null) {
                // Set the unique id to +1 the maximum id value in the column
                transaction.setUniqueID(tname, b_num.longValue() + 1L);
            }
        }

        // Commit and close the transaction.
        try {
            transaction.closeAndCommit();
        } catch (TransactionException e) {
            Debug().writeException(e);
            throw new Error("Transaction Exception creating conglomerate.");
        }
    }

    /**
     * Resets the table sequence id for all the system tables managed by the
     * conglomerate.
     */
    void resetAllSystemTableID() {
        resetTableID(PRIMARY_INFO_TABLE);
        resetTableID(FOREIGN_INFO_TABLE);
        resetTableID(UNIQUE_INFO_TABLE);
        resetTableID(CHECK_INFO_TABLE);
        resetTableID(SCHEMA_INFO_TABLE);
    }

    /**
     * Populates the system table schema with initial data for an empty
     * conglomerate.  This sets up the standard variables and table
     * constraint data.
     */
    private void initializeSystemTableSchema() {
        // Create the transaction
        Transaction transaction = createTransaction();

        // Insert the two default schema names,
        transaction.createSchema(SYSTEM_SCHEMA, "SYSTEM");

        // -- Primary Keys --
        // The 'id' columns are primary keys on all the system tables,
        final String[] id_col = new String[]{"id"};
        transaction.addPrimaryKeyConstraint(PRIMARY_INFO_TABLE,
                id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_PK_PK");
        transaction.addPrimaryKeyConstraint(FOREIGN_INFO_TABLE,
                id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_FK_PK");
        transaction.addPrimaryKeyConstraint(UNIQUE_INFO_TABLE,
                id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_UNIQUE_PK");
        transaction.addPrimaryKeyConstraint(CHECK_INFO_TABLE,
                id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_CHECK_PK");
        transaction.addPrimaryKeyConstraint(SCHEMA_INFO_TABLE,
                id_col, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_SCHEMA_PK");

        // -- Foreign Keys --
        // Create the foreign key references,
        final String[] fk_col = new String[1];
        final String[] fk_ref_col = new String[]{"id"};
        fk_col[0] = "pk_id";
        transaction.addForeignKeyConstraint(
                PRIMARY_COLS_TABLE, fk_col, PRIMARY_INFO_TABLE, fk_ref_col,
                Transaction.NO_ACTION, Transaction.NO_ACTION,
                Transaction.INITIALLY_IMMEDIATE, "SYSTEM_PK_FK");
        fk_col[0] = "fk_id";
        transaction.addForeignKeyConstraint(
                FOREIGN_COLS_TABLE, fk_col, FOREIGN_INFO_TABLE, fk_ref_col,
                Transaction.NO_ACTION, Transaction.NO_ACTION,
                Transaction.INITIALLY_IMMEDIATE, "SYSTEM_FK_FK");
        fk_col[0] = "un_id";
        transaction.addForeignKeyConstraint(
                UNIQUE_COLS_TABLE, fk_col, UNIQUE_INFO_TABLE, fk_ref_col,
                Transaction.NO_ACTION, Transaction.NO_ACTION,
                Transaction.INITIALLY_IMMEDIATE, "SYSTEM_UNIQUE_FK");

        // PKeyInfo 'schema', 'table' column is a unique set,
        // (You are only allowed one primary key per table).
        String[] columns = new String[]{"schema", "table"};
        transaction.addUniqueConstraint(PRIMARY_INFO_TABLE,
                columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_PKEY_ST_UNIQUE");
        // SchemaInfo 'name' column is a unique column,
        columns = new String[]{"name"};
        transaction.addUniqueConstraint(SCHEMA_INFO_TABLE,
                columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_SCHEMA_UNIQUE");
//    columns = new String[] { "name" };
        columns = new String[]{"name", "schema"};
        // PKeyInfo 'name' column is a unique column,
        transaction.addUniqueConstraint(PRIMARY_INFO_TABLE,
                columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_PKEY_UNIQUE");
        // FKeyInfo 'name' column is a unique column,
        transaction.addUniqueConstraint(FOREIGN_INFO_TABLE,
                columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_FKEY_UNIQUE");
        // UniqueInfo 'name' column is a unique column,
        transaction.addUniqueConstraint(UNIQUE_INFO_TABLE,
                columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_UNIQUE_UNIQUE");
        // CheckInfo 'name' column is a unique column,
        transaction.addUniqueConstraint(CHECK_INFO_TABLE,
                columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_CHECK_UNIQUE");

        // DatabaseVars 'variable' is unique
        columns = new String[]{"variable"};
        transaction.addUniqueConstraint(PERSISTENT_VAR_TABLE,
                columns, Transaction.INITIALLY_IMMEDIATE, "SYSTEM_DATABASEVARS_UNIQUE");

        // Insert the version number of the database
        transaction.setPersistentVar("database.version", "1.4");

        // Commit and close the transaction.
        try {
            transaction.closeAndCommit();
        } catch (TransactionException e) {
            Debug().writeException(e);
            throw new Error("Transaction Exception initializing conglomerate.");
        }

    }

    /**
     * Initializes the BlobStore.  If the BlobStore doesn't exist it will be
     * created, and if it does exist it will be initialized.
     */
    private void initializeBlobStore() throws IOException {

        // Does the file already exist?
        boolean blob_store_exists = storeSystem().storeExists("BlobStore");
        // If the blob store doesn't exist and we are read_only, we can't do
        // anything further so simply return.
        if (!blob_store_exists && isReadOnly()) {
            return;
        }

        // The blob store,
        if (blob_store_exists) {
            act_blob_store = storeSystem().openStore("BlobStore");
        } else {
            act_blob_store = storeSystem().createStore("BlobStore");
        }

        try {
            act_blob_store.lockForWrite();

            // Create the BlobStore object
            blob_store = new BlobStore(act_blob_store);

            // Get the 64 byte fixed area
            MutableArea fixed_area = act_blob_store.getMutableArea(-1);
            // If the blob store didn't exist then we need to create it here,
            if (!blob_store_exists) {
                long header_p = blob_store.create();
                fixed_area.putLong(header_p);
                fixed_area.checkOut();
            } else {
                // Otherwise we need to initialize the blob store
                long header_p = fixed_area.getLong();
                blob_store.init(header_p);
            }
        } finally {
            act_blob_store.unlockForWrite();
        }

    }


    // ---------- Private methods ----------

    /**
     * Returns true if the system is in read only mode.
     */
    private boolean isReadOnly() {
        return system.readOnlyAccess();
    }

    /**
     * Returns the path of the database.
     */
    private File getPath() {
        return system.getDatabasePath();
    }

    /**
     * Returns the next unique table_id value for a new table and updates the
     * conglomerate state information as appropriate.
     */
    private int nextUniqueTableID() throws IOException {
        return state_store.nextTableID();
    }


    /**
     * Sets up the internal state of this object.
     */
    private void setupInternal() {
        commit_id = 0;
        table_list = new ArrayList();

//    // If the VM supports shutdown hook,
//    try {
//      shutdown_hook = new ConglomerateShutdownHookThread();
//      Runtime.getRuntime().addShutdownHook(shutdown_hook);
//    }
//    catch (Throwable e) {
//      // Catch instantiation/access errors
//      system.Debug().write(Lvl.MESSAGE, this,
//                           "Unable to register shutdown hook.");
//    }

    }

    // ---------- Public methods ----------

    /**
     * Minimally creates a new conglomerate but does NOT initialize any of the
     * system tables.  This is a useful feature for a copy function that requires
     * a TableDataConglomerate object to copy data into but does not require any
     * initial system tables (because this information is copied from the source
     * conglomerate.
     */
    void minimalCreate(String name) throws IOException {
        this.name = name;

        if (exists(name)) {
            throw new IOException("Conglomerate already exists: " + name);
        }

        // Lock the store system (generates an IOException if exclusive lock
        // can not be made).
        if (!isReadOnly()) {
            storeSystem().lock(name);
        }

        // Create/Open the state store
        act_state_store = storeSystem().createStore(name + STATE_POST);
        try {
            act_state_store.lockForWrite();

            state_store = new StateStore(act_state_store);
            long head_p = state_store.create();
            // Get the fixed area
            MutableArea fixed_area = act_state_store.getMutableArea(-1);
            fixed_area.putLong(head_p);
            fixed_area.checkOut();
        } finally {
            act_state_store.unlockForWrite();
        }

        setupInternal();

        // Init the conglomerate blob store
        initializeBlobStore();

        // Create the system table (but don't initialize)
        updateSystemTableSchema();

    }

    /**
     * Creates a new conglomerate at the given path in the file system.  This
     * must be an empty directory where files can be stored.  This will create
     * the conglomerate and exit in an open (read/write) state.
     */
    public void create(String name) throws IOException {
        minimalCreate(name);

        // Initialize the conglomerate system tables.
        initializeSystemTableSchema();

        // Commit the state
        state_store.commit();

    }

    /**
     * Opens a conglomerate.  If the conglomerate does not exist then an
     * IOException is generated.  Once a conglomerate is open, we may start
     * opening transactions and altering the data within it.
     */
    public void open(String name) throws IOException {
        this.name = name;

        if (!exists(name)) {
            throw new IOException("Conglomerate doesn't exists: " + name);
        }

        // Check the file lock
        if (!isReadOnly()) {
            // Obtain the lock (generate error if this is not possible)
            storeSystem().lock(name);
        }

        // Open the state store
        act_state_store = storeSystem().openStore(name + STATE_POST);
        state_store = new StateStore(act_state_store);
        // Get the fixed 64 byte area.
        Area fixed_area = act_state_store.getArea(-1);
        long head_p = fixed_area.getLong();
        state_store.init(head_p);

        setupInternal();

        // Init the conglomerate blob store
        initializeBlobStore();

        readVisibleTables();
        readDroppedTables();

        // We possibly have things to clean up if there are deleted columns.
        cleanUpConglomerate();

    }

    /**
     * Closes this conglomerate.  The conglomerate must be open for it to be
     * closed.  When closed, any use of this object is undefined.
     */
    public void close() throws IOException {
        synchronized (commit_lock) {

            // We possibly have things to clean up.
            cleanUpConglomerate();

            // Set a check point
            store_system.setCheckPoint();

            // Go through and close all the committed tables.
            int size = table_list.size();
            for (int i = 0; i < size; ++i) {
                MasterTableDataSource master =
                        (MasterTableDataSource) table_list.get(i);
                master.dispose(false);
            }

            state_store.commit();
            storeSystem().closeStore(act_state_store);

            table_list = null;

        }

        // Unlock the storage system
        storeSystem().unlock(name);

        if (blob_store != null) {
            storeSystem().closeStore(act_blob_store);
        }

//    removeShutdownHook();
    }

//  /**
//   * Removes the shutdown hook.
//   */
//  private void removeShutdownHook() {
//    // If the VM supports shutdown hook, remove it,
//    try {
//      if (shutdown_hook != null) {
////        System.out.println("REMOVING: " + this);
//        Runtime.getRuntime().removeShutdownHook(shutdown_hook);
//        // We have no start it otherwise the ThreadGroup won't remove its
//        // reference to it and it causes GC problems.
//        shutdown_hook.start();
//        shutdown_hook.waitUntilComplete();
//        shutdown_hook = null;
//      }
//    }
//    catch (Throwable e) {
//      // Catch (and ignore) instantiation/access errors
//    }
//  }

    /**
     * Deletes and closes the conglomerate.  This will delete all the files in
     * the file system associated with this conglomerate, so this method should
     * be used with care.
     * <p>
     * WARNING: Will result in total loss of all data stored in the conglomerate.
     */
    public void delete() throws IOException {
        synchronized (commit_lock) {

            // We possibly have things to clean up.
            cleanUpConglomerate();

            // Go through and delete and close all the committed tables.
            int size = table_list.size();
            for (int i = 0; i < size; ++i) {
                MasterTableDataSource master =
                        (MasterTableDataSource) table_list.get(i);
                master.drop();
            }

            // Delete the state file
            state_store.commit();
            storeSystem().closeStore(act_state_store);
            storeSystem().deleteStore(act_state_store);

            // Delete the blob store
            if (blob_store != null) {
                storeSystem().closeStore(act_blob_store);
                storeSystem().deleteStore(act_blob_store);
            }

            // Invalidate this object
            table_list = null;

        }

        // Unlock the storage system.
        storeSystem().unlock(name);
    }

    /**
     * Returns true if the conglomerate is closed.
     */
    public boolean isClosed() {
        synchronized (commit_lock) {
            return table_list == null;
        }
    }


    /**
     * Returns true if the conglomerate exists in the file system and can
     * be opened.
     */
    public boolean exists(String name) throws IOException {
        return storeSystem().storeExists(name + STATE_POST);
    }

    /**
     * Makes a complete copy of this database to the position represented by the
     * given TableDataConglomerate object.  The given TableDataConglomerate
     * object must NOT be being used by another database running in the JVM.
     * This may take a while to complete.  The backup operation occurs within its
     * own transaction and the copy transaction is read-only meaning there is no
     * way for the copy process to interfere with other transactions running
     * concurrently.
     * <p>
     * The conglomerate must be open before this method is called.
     */
    public void liveCopyTo(TableDataConglomerate dest_conglomerate)
            throws IOException {

        // The destination store system
        StoreSystem dest_store_system = dest_conglomerate.storeSystem();

        // Copy all the blob data from the given blob store to the current blob
        // store.
        dest_conglomerate.blob_store.copyFrom(dest_store_system, blob_store);

        // Open new transaction - this is the current view we are going to copy.
        Transaction transaction = createTransaction();

        try {

            // Copy the data in this transaction to the given destination store system.
            transaction.liveCopyAllDataTo(dest_conglomerate);

        } finally {
            // Make sure we close the transaction
            try {
                transaction.closeAndCommit();
            } catch (TransactionException e) {
                throw new RuntimeException("Transaction Error: " + e.getMessage());
            }
        }

        // Finished - increment the live copies counter.
        getSystem().stats().increment("TableDataConglomerate.liveCopies");

    }

    // ---------- Diagnostic and repair ----------

    /**
     * Returns a RawDiagnosticTable object that is used for diagnostics of the
     * table with the given file name.
     */
    public RawDiagnosticTable getDiagnosticTable(String table_file_name) {
        synchronized (commit_lock) {
            for (int i = 0; i < table_list.size(); ++i) {
                MasterTableDataSource master =
                        (MasterTableDataSource) table_list.get(i);
                if (master.getSourceIdent().equals(table_file_name)) {
                    return master.getRawDiagnosticTable();
                }
            }
        }
        return null;
    }

    /**
     * Returns the list of file names for all tables in this conglomerate.
     */
    public String[] getAllTableFileNames() {
        synchronized (commit_lock) {
            String[] list = new String[table_list.size()];
            for (int i = 0; i < table_list.size(); ++i) {
                MasterTableDataSource master =
                        (MasterTableDataSource) table_list.get(i);
                list[i] = master.getSourceIdent();
            }
            return list;
        }
    }

    // ---------- Conglomerate event notification ----------

    /**
     * Adds a listener for transactional modification events that occur on the
     * given table in this conglomerate.  A transactional modification event is
     * an event fired immediately upon the modification of a table by a
     * transaction, either immediately before the modification or immediately
     * after.  Also an event is fired when a modification to a table is
     * successfully committed.
     * <p>
     * The BEFORE_* type triggers are given the opportunity to modify the
     * contents of the RowData before the update or insert occurs.  All triggers
     * may generate an exception which will cause the transaction to rollback.
     * <p>
     * The event carries with it the event type, the transaction that the event
     * occurred in, and any information regarding the modification itself.
     * <p>
     * This event/listener mechanism is intended to be used to implement higher
     * layer database triggering systems.  Note that care must be taken with
     * the commit level events because they occur inside a commit lock on this
     * conglomerate and so synchronization and deadlock issues need to be
     * carefully considered.
     * <p>
     * NOTE: A listener on the given table will be notified of ALL table
     *  modification events by all transactions at the time they happen.
     *
     * @param table_name the name of the table in the conglomerate to listen for
     *   events from.
     * @param listener the listener to be notified of events.
     */
    public void addTransactionModificationListener(TableName table_name,
                                                   TransactionModificationListener listener) {
        synchronized (modification_listeners) {
            ArrayList list = (ArrayList) modification_listeners.get(table_name);
            if (list == null) {
                // If the mapping doesn't exist then create the list for the table
                // here.
                list = new ArrayList();
                modification_listeners.put(table_name, list);
            }

            list.add(listener);
        }
    }

    /**
     * Removes a listener for transaction modification events on the given table
     * in this conglomerate as previously set by the
     * 'addTransactionModificationListener' method.
     *
     * @param table_name the name of the table in the conglomerate to remove from
     *   the listener list.
     * @param listener the listener to be removed.
     */
    public void removeTransactionModificationListener(TableName table_name,
                                                      TransactionModificationListener listener) {
        synchronized (modification_listeners) {
            ArrayList list = (ArrayList) modification_listeners.get(table_name);
            if (list != null) {
                int sz = list.size();
                for (int i = sz - 1; i >= 0; --i) {
                    if (list.get(i) == listener) {
                        list.remove(i);
                    }
                }
            }
        }
    }

    // ---------- Transactional management ----------

    /**
     * Starts a new transaction.  The Transaction object returned by this
     * method is used to read the contents of the database at the time
     * the transaction was started.  It is also used if any modifications are
     * required to be made.
     */
    public Transaction createTransaction() {
        long this_commit_id;
        ArrayList this_committed_tables = new ArrayList();

        // Don't let a commit happen while we are looking at this.
        synchronized (commit_lock) {

            this_commit_id = commit_id;
            StateResource[] committed_table_list = state_store.getVisibleList();
            for (int i = 0; i < committed_table_list.length; ++i) {
                this_committed_tables.add(
                        getMasterTable((int) committed_table_list[i].table_id));
            }

            // Create a set of IndexSet for all the tables in this transaction.
            int sz = this_committed_tables.size();
            ArrayList index_info = new ArrayList(sz);
            for (int i = 0; i < sz; ++i) {
                MasterTableDataSource mtable =
                        (MasterTableDataSource) this_committed_tables.get(i);
                index_info.add(mtable.createIndexSet());
            }

            // Create the transaction and record it in the open transactions list.
            Transaction t = new Transaction(this,
                    this_commit_id, this_committed_tables, index_info);
            open_transactions.addTransaction(t);
            return t;

        }

    }

    /**
     * This is called to notify the conglomerate that the transaction has
     * closed.  This is always called from either the rollback or commit method
     * of the transaction object.
     * <p>
     * NOTE: This increments 'commit_id' and requires that the conglomerate is
     *   commit locked.
     */
    private void closeTransaction(Transaction transaction) {
        boolean last_transaction = false;
        // Closing must happen under a commit lock.
        synchronized (commit_lock) {
            open_transactions.removeTransaction(transaction);
            // Increment the commit id.
            ++commit_id;
            // Was that the last transaction?
            last_transaction = open_transactions.count() == 0;
        }

        // If last transaction then schedule a clean up event.
        if (last_transaction) {
            try {
                cleanUpConglomerate();
            } catch (IOException e) {
                Debug().write(Lvl.ERROR, this, "Error cleaning up conglomerate");
                Debug().writeException(Lvl.ERROR, e);
            }
        }

    }


    /**
     * Closes and drops the MasterTableDataSource.  This should only be called
     * from the clean up method (cleanUpConglomerate()).
     * <p>
     * Returns true if the drop succeeded.  A drop may fail if, for example, the
     * roots of the table are locked.
     * <p>
     * Note that the table_file_name will be encoded with the table type.  For
     * example, ":2mighty.pony"
     */
    private boolean closeAndDropTable(String table_file_name) throws IOException {
        // Find the table with this file name.
        for (int i = 0; i < table_list.size(); ++i) {
            MasterTableDataSource t = (MasterTableDataSource) table_list.get(i);
            String enc_fn = table_file_name.substring(2);
            if (t.getSourceIdent().equals(enc_fn)) {
                // Close and remove from the list.
                if (t.isRootLocked()) {
                    // We can't drop a table that has roots locked..
                    return false;
                }

                // This drops if the table has been marked as being dropped.
                boolean b = t.drop();
                if (b) {
                    table_list.remove(i);
                }
                return b;
            }
        }
        return false;
    }

    /**
     * Closes the MasterTableDataSource with the given source ident.  This should
     * only be called from the clean up method (cleanUpConglomerate()).
     * <p>
     * Note that the table_file_name will be encoded with the table type.  For
     * example, ":2mighty.pony"
     */
    private void closeTable(String table_file_name, boolean pending_drop)
            throws IOException {
        // Find the table with this file name.
        for (int i = 0; i < table_list.size(); ++i) {
            MasterTableDataSource t = (MasterTableDataSource) table_list.get(i);
            String enc_fn = table_file_name.substring(2);
            if (t.getSourceIdent().equals(enc_fn)) {
                // Close and remove from the list.
                if (t.isRootLocked()) {
                    // We can't drop a table that has roots locked..
                    return;
                }

                // This closes the table
                t.dispose(pending_drop);
                return;
            }
        }
        return;
    }

    /**
     * Cleans up the conglomerate by deleting all tables marked as deleted.
     * This should be called when the conglomerate is opened, shutdown and
     * when there are no transactions open.
     */
    private void cleanUpConglomerate() throws IOException {
        synchronized (commit_lock) {
            if (isClosed()) {
                return;
            }

            // If no open transactions on the database, then clean up.
            if (open_transactions.count() == 0) {

                StateResource[] delete_list = state_store.getDeleteList();
                if (delete_list.length > 0) {
                    int drop_count = 0;

                    for (int i = delete_list.length - 1; i >= 0; --i) {
                        String fn = (String) delete_list[i].name;
                        closeTable(fn, true);
                    }

//          // NASTY HACK: The native win32 file mapping will not
//          //   let you delete a file that is mapped.  The NIO API does not allow
//          //   you to manually unmap a file, and the only way to unmap
//          //   memory under win32 is to wait for the garbage collector to
//          //   free it.  So this is a hack to try and make the engine
//          //   unmap the memory mapped buffer.
//          //
//          //   This is not a problem under Unix/Linux because the OS has no
//          //   difficulty deleting a file that is mapped.
//
//          System.gc();
//          try {
//            Thread.sleep(5);
//          }
//          catch (InterruptedException e) { /* ignore */ }

                    for (int i = delete_list.length - 1; i >= 0; --i) {
                        String fn = (String) delete_list[i].name;
                        boolean dropped = closeAndDropTable(fn);
                        // If we managed to drop the table, remove from the list.
                        if (dropped) {
                            state_store.removeDeleteResource(fn);
                            ++drop_count;
                        }
                    }

                    // If we dropped a table, commit an update to the conglomerate state.
                    if (drop_count > 0) {
                        state_store.commit();
                    }
                }

            }
        }
    }

    // ---------- Detection of constraint violations ----------

    /**
     * A variable resolver for a single row of a table source.  Used when
     * evaluating a check constraint for newly added row.
     */
    private static class TableRowVariableResolver implements VariableResolver {

        private TableDataSource table;
        private int row_index = -1;

        public TableRowVariableResolver(TableDataSource table, int row) {
            this.table = table;
            this.row_index = row;
        }

        private int findColumnName(Variable variable) {
            int col_index = table.getDataTableDef().findColumnName(
                    variable.getName());
            if (col_index == -1) {
                throw new Error("Can't find column: " + variable);
            }
            return col_index;
        }

        // --- Implemented ---

        public int setID() {
            return row_index;
        }

        public TObject resolve(Variable variable) {
            int col_index = findColumnName(variable);
            return table.getCellContents(col_index, row_index);
        }

        public TType returnTType(Variable variable) {
            int col_index = findColumnName(variable);
            return table.getDataTableDef().columnAt(col_index).getTType();
        }

    }

    /**
     * Convenience, converts a String[] array to a comma deliminated string
     * list.
     */
    static String stringColumnList(String[] list) {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < list.length - 1; ++i) {
            buf.append(list[i]);
        }
        buf.append(list[list.length - 1]);
        return new String(buf);
    }

    /**
     * Convenience, returns either 'Immediate' or 'Deferred' dependant on the
     * deferred short.
     */
    static String deferredString(short deferred) {
        switch (deferred) {
            case (Transaction.INITIALLY_IMMEDIATE):
                return "Immediate";
            case (Transaction.INITIALLY_DEFERRED):
                return "Deferred";
            default:
                throw new Error("Unknown deferred string.");
        }
    }

    /**
     * Returns a list of column indices into the given DataTableDef for the
     * given column names.
     */
    static int[] findColumnIndices(DataTableDef table_def, String[] cols) {
        // Resolve the list of column names to column indexes
        int[] col_indexes = new int[cols.length];
        for (int i = 0; i < cols.length; ++i) {
            col_indexes[i] = table_def.findColumnName(cols[i]);
        }
        return col_indexes;
    }

    /**
     * Checks the uniqueness of the columns in the row of the table.  If
     * the given column information in the row data is not unique then it
     * returns false.  We also check for a NULL values - a PRIMARY KEY constraint
     * does not allow NULL values, whereas a UNIQUE constraint does.
     */
    private static boolean isUniqueColumns(
            TableDataSource table, int rindex, String[] cols,
            boolean nulls_are_allowed) {

        DataTableDef table_def = table.getDataTableDef();
        // 'identical_rows' keeps a tally of the rows that match our added cell.
        IntegerVector identical_rows = null;

        // Resolve the list of column names to column indexes
        int[] col_indexes = findColumnIndices(table_def, cols);

        // If the value being tested for uniqueness contains NULL, we return true
        // if nulls are allowed.
        for (int i = 0; i < col_indexes.length; ++i) {
            TObject cell = table.getCellContents(col_indexes[i], rindex);
            if (cell.isNull()) {
                return nulls_are_allowed;
            }
        }


        for (int i = 0; i < col_indexes.length; ++i) {

            int col_index = col_indexes[i];

            // Get the column definition and the cell being inserted,
//      DataTableColumnDef column_def = table_def.columnAt(col_index);
            TObject cell = table.getCellContents(col_index, rindex);

            // We are assured of uniqueness if 'identical_rows != null &&
            // identical_rows.size() == 0'  This is because 'identical_rows' keeps
            // a running tally of the rows in the table that contain unique columns
            // whose cells match the record being added.

            if (identical_rows == null || identical_rows.size() > 0) {

                // Ask SelectableScheme to return pointers to row(s) if there is
                // already a cell identical to this in the table.

                SelectableScheme ss = table.getColumnScheme(col_index);
                IntegerVector ivec = ss.selectEqual(cell);

                // If 'identical_rows' hasn't been set up yet then set it to 'ivec'
                // (the list of rows where there is a cell which is equal to the one
                //  being added)
                // If 'identical_rows' has been set up, then perform an
                // 'intersection' operation on the two lists (only keep the numbers
                // that are repeated in both lists).  Therefore we keep the rows
                // that match the row being added.

                if (identical_rows == null) {
                    identical_rows = ivec;
                } else {
                    ivec.quickSort();
                    int row_index = identical_rows.size() - 1;
                    while (row_index >= 0) {
                        int val = identical_rows.intAt(row_index);
                        int found_index = ivec.sortedIndexOf(val);
                        // If we _didn't_ find the index in the array
                        if (found_index >= ivec.size() ||
                                ivec.intAt(found_index) != val) {
                            identical_rows.removeIntAt(row_index);
                        }
                        --row_index;
                    }
                }

            }

        } // for each column

        // If there is 1 (the row we added) then we are unique, otherwise we are
        // not.
        if (identical_rows != null) {
            int sz = identical_rows.size();
            if (sz == 1) {
                return true;
            }
            if (sz > 1) {
                return false;
            } else if (sz == 0) {
                throw new Error("Assertion failed: We must be able to find the " +
                        "row we are testing uniqueness against!");
            }
        }
        return true;

    }


    /**
     * Returns the key indices found in the given table.  The keys are
     * in the given column indices, and the key is in the 'key' array.  This can
     * be used to count the number of keys found in a table for constraint
     * violation checking.
     */
    static IntegerVector findKeys(TableDataSource t2, int[] col2_indexes,
                                  TObject[] key_value) {

        int key_size = key_value.length;
        // Now query table 2 to determine if the key values are present.
        // Use index scan on first key.
        SelectableScheme ss = t2.getColumnScheme(col2_indexes[0]);
        IntegerVector list = ss.selectEqual(key_value[0]);
        if (key_size > 1) {
            // Full scan for the rest of the columns
            int sz = list.size();
            // For each element of the list
            for (int i = sz - 1; i >= 0; --i) {
                int r_index = list.intAt(i);
                // For each key in the column list
                for (int c = 1; c < key_size; ++c) {
                    int col_index = col2_indexes[c];
                    TObject c_value = key_value[c];
                    if (c_value.compareTo(t2.getCellContents(col_index, r_index)) != 0) {
                        // If any values in the key are not equal set this flag to false
                        // and remove the index from the list.
                        list.removeIntAt(i);
                        // Break the for loop
                        break;
                    }
                }
            }
        }

        return list;
    }

    /**
     * Finds the number of rows that are referenced between the given row of
     * table1 and that match table2.  This method is used to determine if
     * there are referential links.
     * <p>
     * If this method returns -1 it means the value being searched for is NULL
     * therefore we can't determine if there are any referenced links.
     * <p>
     * HACK: If 'check_source_table_key' is set then the key is checked for in
     * the source table and if it exists returns 0.  Otherwise it looks for
     * references to the key in table2.
     */
    private static int rowCountOfReferenceTable(
            SimpleTransaction transaction,
            int row_index, TableName table1, String[] cols1,
            TableName table2, String[] cols2,
            boolean check_source_table_key) {

        // Get the tables
        TableDataSource t1 = transaction.getTableDataSource(table1);
        TableDataSource t2 = transaction.getTableDataSource(table2);
        // The table defs
        DataTableDef dtd1 = t1.getDataTableDef();
        DataTableDef dtd2 = t2.getDataTableDef();
        // Resolve the list of column names to column indexes
        int[] col1_indexes = findColumnIndices(dtd1, cols1);
        int[] col2_indexes = findColumnIndices(dtd2, cols2);

        int key_size = col1_indexes.length;
        // Get the data from table1
        TObject[] key_value = new TObject[key_size];
        int null_count = 0;
        for (int n = 0; n < key_size; ++n) {
            key_value[n] = t1.getCellContents(col1_indexes[n], row_index);
            if (key_value[n].isNull()) {
                ++null_count;
            }
        }

        // If we are searching for null then return -1;
        if (null_count > 0) {
            return -1;
        }

        // HACK: This is a hack.  The purpose is if the key exists in the source
        //   table we return 0 indicating to the delete check that there are no
        //   references and it's valid.  To the semantics of the method this is
        //   incorrect.
        if (check_source_table_key) {
            IntegerVector keys = findKeys(t1, col1_indexes, key_value);
            int key_count = keys.size();
            if (key_count > 0) {
                return 0;
            }
        }

        return findKeys(t2, col2_indexes, key_value).size();
    }


    /**
     * Checks that the nullibility and class of the fields in the given
     * rows are valid.  Should be used as part of the insert procedure.
     */
    static void checkFieldConstraintViolations(
            SimpleTransaction transaction,
            TableDataSource table, int[] row_indices) {

        // Quick exit case
        if (row_indices == null || row_indices.length == 0) {
            return;
        }

        // Check for any bad cells - which are either cells that are 'null' in a
        // column declared as 'not null', or duplicated in a column declared as
        // unique.

        DataTableDef table_def = table.getDataTableDef();
        TableName table_name = table_def.getTableName();

        // Check not-null columns are not null.  If they are null, throw an
        // error.  Additionally check that JAVA_OBJECT columns are correctly
        // typed.

        // Check each field of the added rows
        int len = table_def.columnCount();
        for (int i = 0; i < len; ++i) {

            // Get the column definition and the cell being inserted,
            DataTableColumnDef column_def = table_def.columnAt(i);
            // For each row added to this column
            for (int rn = 0; rn < row_indices.length; ++rn) {
                TObject cell = table.getCellContents(i, row_indices[rn]);

                // Check: Column defined as not null and cell being inserted is
                // not null.
                if (column_def.isNotNull() && cell.isNull()) {
                    throw new DatabaseConstraintViolationException(
                            DatabaseConstraintViolationException.NULLABLE_VIOLATION,
                            "You tried to add 'null' cell to column '" +
                                    table_def.columnAt(i).getName() +
                                    "' which is declared as 'not_null'");
                }

                // Check: If column is a java object, then deserialize and check the
                //        object is an instance of the class constraint,
                if (!cell.isNull() &&
                        column_def.getSQLType() ==
                                com.pony.database.global.SQLTypes.JAVA_OBJECT) {
                    String class_constraint = column_def.getClassConstraint();
                    // Everything is derived from java.lang.Object so this optimization
                    // will not cause an object deserialization.
                    if (!class_constraint.equals("java.lang.Object")) {
                        // Get the binary representation of the java object
                        ByteLongObject serialized_jobject =
                                (ByteLongObject) cell.getObject();
                        // Deserialize the object
                        Object ob = ObjectTranslator.deserialize(serialized_jobject);
                        // Check it's assignable from the constraining class
                        if (!ob.getClass().isAssignableFrom(
                                column_def.getClassConstraintAsClass())) {
                            throw new DatabaseConstraintViolationException(
                                    DatabaseConstraintViolationException.JAVA_TYPE_VIOLATION,
                                    "The Java object being inserted is not derived from the " +
                                            "class constraint defined for the column (" +
                                            class_constraint + ")");
                        }
                    }
                }

            } // For each row being added

        } // for each column

    }

    /**
     * Performs constraint violation checks on an addition of the given set of
     * row indices into the TableDataSource in the given transaction.  If a
     * violation is detected a DatabaseConstraintViolationException is thrown.
     * <p>
     * If deferred = IMMEDIATE only immediate constraints are tested.  If
     * deferred = DEFERRED all constraints are tested.
     *
     * @param transaction the Transaction instance used to determine table
     *   constraints.
     * @param table the table to test
     * @param row_indices the list of rows that were added to the table.
     * @param deferred '1' indicates Transaction.IMMEDIATE,
     *   '2' indicates Transaction.DEFERRED.
     */
    static void checkAddConstraintViolations(
            SimpleTransaction transaction,
            TableDataSource table, int[] row_indices, short deferred) {

        String cur_schema = table.getDataTableDef().getSchema();
        QueryContext context = new SystemQueryContext(transaction, cur_schema);

        // Quick exit case
        if (row_indices == null || row_indices.length == 0) {
            return;
        }

        DataTableDef table_def = table.getDataTableDef();
        TableName table_name = table_def.getTableName();

        // ---- Constraint checking ----

        // Check any primary key constraint.
        Transaction.ColumnGroup primary_key =
                Transaction.queryTablePrimaryKeyGroup(transaction, table_name);
        if (primary_key != null &&
                (deferred == Transaction.INITIALLY_DEFERRED ||
                        primary_key.deferred == Transaction.INITIALLY_IMMEDIATE)) {

            // For each row added to this column
            for (int rn = 0; rn < row_indices.length; ++rn) {
                if (!isUniqueColumns(table, row_indices[rn],
                        primary_key.columns, false)) {
                    throw new DatabaseConstraintViolationException(
                            DatabaseConstraintViolationException.PRIMARY_KEY_VIOLATION,
                            deferredString(deferred) + " primary Key constraint violation (" +
                                    primary_key.name + ") Columns = ( " +
                                    stringColumnList(primary_key.columns) +
                                    " ) Table = ( " + table_name.toString() + " )");
                }
            } // For each row being added

        }

        // Check any unique constraints.
        Transaction.ColumnGroup[] unique_constraints =
                Transaction.queryTableUniqueGroups(transaction, table_name);
        for (int i = 0; i < unique_constraints.length; ++i) {
            Transaction.ColumnGroup unique = unique_constraints[i];
            if (deferred == Transaction.INITIALLY_DEFERRED ||
                    unique.deferred == Transaction.INITIALLY_IMMEDIATE) {

                // For each row added to this column
                for (int rn = 0; rn < row_indices.length; ++rn) {
                    if (!isUniqueColumns(table, row_indices[rn], unique.columns, true)) {
                        throw new DatabaseConstraintViolationException(
                                DatabaseConstraintViolationException.UNIQUE_VIOLATION,
                                deferredString(deferred) + " unique constraint violation (" +
                                        unique.name + ") Columns = ( " +
                                        stringColumnList(unique.columns) + " ) Table = ( " +
                                        table_name.toString() + " )");
                    }
                } // For each row being added

            }
        }

        // Check any foreign key constraints.
        // This ensures all foreign references in the table are referenced
        // to valid records.
        Transaction.ColumnGroupReference[] foreign_constraints =
                Transaction.queryTableForeignKeyReferences(transaction, table_name);
        for (int i = 0; i < foreign_constraints.length; ++i) {
            Transaction.ColumnGroupReference ref = foreign_constraints[i];
            if (deferred == Transaction.INITIALLY_DEFERRED ||
                    ref.deferred == Transaction.INITIALLY_IMMEDIATE) {
                // For each row added to this column
                for (int rn = 0; rn < row_indices.length; ++rn) {
                    // Make sure the referenced record exists

                    // Return the count of records where the given row of
                    //   table_name(columns, ...) IN
                    //                    ref_table_name(ref_columns, ...)
                    int row_count = rowCountOfReferenceTable(transaction,
                            row_indices[rn],
                            ref.key_table_name, ref.key_columns,
                            ref.ref_table_name, ref.ref_columns,
                            false);
                    if (row_count == -1) {
                        // foreign key is NULL
                    }
                    if (row_count == 0) {
                        throw new DatabaseConstraintViolationException(
                                DatabaseConstraintViolationException.FOREIGN_KEY_VIOLATION,
                                deferredString(deferred) + " foreign key constraint violation (" +
                                        ref.name + ") Columns = " +
                                        ref.key_table_name.toString() + "( " +
                                        stringColumnList(ref.key_columns) + " ) -> " +
                                        ref.ref_table_name.toString() + "( " +
                                        stringColumnList(ref.ref_columns) + " )");
                    }
                } // For each row being added.
            }
        }

        // Any general checks of the inserted data
        Transaction.CheckExpression[] check_constraints =
                Transaction.queryTableCheckExpressions(transaction, table_name);

        // The TransactionSystem object
        TransactionSystem system = transaction.getSystem();

        // For each check constraint, check that it evaluates to true.
        for (int i = 0; i < check_constraints.length; ++i) {
            Transaction.CheckExpression check = check_constraints[i];
            if (deferred == Transaction.INITIALLY_DEFERRED ||
                    check.deferred == Transaction.INITIALLY_IMMEDIATE) {

                check = system.prepareTransactionCheckConstraint(table_def, check);
                Expression exp = check.expression;

                // For each row being added to this column
                for (int rn = 0; rn < row_indices.length; ++rn) {
                    TableRowVariableResolver resolver =
                            new TableRowVariableResolver(table, row_indices[rn]);
                    TObject ob = exp.evaluate(null, resolver, context);
                    Boolean b = ob.toBoolean();

                    if (b != null) {
                        if (b.equals(Boolean.FALSE)) {
                            // Evaluated to false so don't allow this row to be added.
                            throw new DatabaseConstraintViolationException(
                                    DatabaseConstraintViolationException.CHECK_VIOLATION,
                                    deferredString(deferred) + " check constraint violation (" +
                                            check.name + ") - '" + exp.text() +
                                            "' evaluated to false for inserted/updated row.");
                        }
                    } else {
                        // NOTE: This error will pass the row by default
                        transaction.Debug().write(Lvl.ERROR,
                                TableDataConglomerate.class,
                                deferredString(deferred) + " check constraint violation (" +
                                        check.name + ") - '" + exp.text() +
                                        "' returned a non boolean or NULL result.");
                    }
                } // For each row being added

            }
        }


    }

    /**
     * Performs constraint violation checks on an addition of the given
     * row index into the TableDataSource in the given transaction.  If a
     * violation is detected a DatabaseConstraintViolationException is thrown.
     * <p>
     * If deferred = IMMEDIATE only immediate constraints are tested.  If
     * deferred = DEFERRED all constraints are tested.
     *
     * @param transaction the Transaction instance used to determine table
     *   constraints.
     * @param table the table to test
     * @param row_index the row that was added to the table.
     * @param deferred '1' indicates Transaction.IMMEDIATE,
     *   '2' indicates Transaction.DEFERRED.
     */
    static void checkAddConstraintViolations(
            SimpleTransaction transaction,
            TableDataSource table, int row_index, short deferred) {
        checkAddConstraintViolations(transaction, table,
                new int[]{row_index}, deferred);
    }

    /**
     * Performs constraint violation checks on a removal of the given set of
     * row indexes from the TableDataSource in the given transaction.  If a
     * violation is detected a DatabaseConstraintViolationException is thrown.
     * <p>
     * If deferred = IMMEDIATE only immediate constraints are tested.  If
     * deferred = DEFERRED all constraints are tested.
     *
     * @param transaction the Transaction instance used to determine table
     *   constraints.
     * @param table the table to test
     * @param row_indices the set of rows that were removed from the table.
     * @param deferred '1' indicates Transaction.IMMEDIATE,
     *   '2' indicates Transaction.DEFERRED.
     */
    static void checkRemoveConstraintViolations(
            SimpleTransaction transaction, TableDataSource table,
            int[] row_indices, short deferred) {

        // Quick exit case
        if (row_indices == null || row_indices.length == 0) {
            return;
        }

        DataTableDef table_def = table.getDataTableDef();
        TableName table_name = table_def.getTableName();

        // Check any imported foreign key constraints.
        // This ensures that a referential reference can not be removed making
        // it invalid.
        Transaction.ColumnGroupReference[] foreign_constraints =
                Transaction.queryTableImportedForeignKeyReferences(
                        transaction, table_name);
        for (int i = 0; i < foreign_constraints.length; ++i) {
            Transaction.ColumnGroupReference ref = foreign_constraints[i];
            if (deferred == Transaction.INITIALLY_DEFERRED ||
                    ref.deferred == Transaction.INITIALLY_IMMEDIATE) {
                // For each row removed from this column
                for (int rn = 0; rn < row_indices.length; ++rn) {
                    // Make sure the referenced record exists

                    // Return the count of records where the given row of
                    //   ref_table_name(columns, ...) IN
                    //                    table_name(ref_columns, ...)
                    int row_count = rowCountOfReferenceTable(transaction,
                            row_indices[rn],
                            ref.ref_table_name, ref.ref_columns,
                            ref.key_table_name, ref.key_columns,
                            true);
                    // There must be 0 references otherwise the delete isn't allowed to
                    // happen.
                    if (row_count > 0) {
                        throw new DatabaseConstraintViolationException(
                                DatabaseConstraintViolationException.FOREIGN_KEY_VIOLATION,
                                deferredString(deferred) + " foreign key constraint violation " +
                                        "on delete (" +
                                        ref.name + ") Columns = " +
                                        ref.key_table_name.toString() + "( " +
                                        stringColumnList(ref.key_columns) + " ) -> " +
                                        ref.ref_table_name.toString() + "( " +
                                        stringColumnList(ref.ref_columns) + " )");
                    }
                } // For each row being added.
            }
        }

    }

    /**
     * Performs constraint violation checks on a removal of the given
     * row index from the TableDataSource in the given transaction.  If a
     * violation is detected a DatabaseConstraintViolationException is thrown.
     * <p>
     * If deferred = IMMEDIATE only immediate constraints are tested.  If
     * deferred = DEFERRED all constraints are tested.
     *
     * @param transaction the Transaction instance used to determine table
     *   constraints.
     * @param table the table to test
     * @param row_index the row that was removed from the table.
     * @param deferred '1' indicates Transaction.IMMEDIATE,
     *   '2' indicates Transaction.DEFERRED.
     */
    static void checkRemoveConstraintViolations(
            SimpleTransaction transaction,
            TableDataSource table, int row_index, short deferred) {
        checkRemoveConstraintViolations(transaction, table,
                new int[]{row_index}, deferred);
    }

    /**
     * Performs constraint violation checks on all the rows in the given
     * table.  If a violation is detected a DatabaseConstraintViolationException
     * is thrown.
     * <p>
     * This method is useful when the constraint schema of a table changes and
     * we need to check existing data in a table is conformant with the new
     * constraint changes.
     * <p>
     * If deferred = IMMEDIATE only immediate constraints are tested.  If
     * deferred = DEFERRED all constraint are tested.
     */
    static void checkAllAddConstraintViolations(
            SimpleTransaction transaction, TableDataSource table,
            short deferred) {
        // Get all the rows in the table
        int[] rows = new int[table.getRowCount()];
        RowEnumeration row_enum = table.rowEnumeration();
        int p = 0;
        while (row_enum.hasMoreRows()) {
            rows[p] = row_enum.nextRowIndex();
            ++p;
        }
        // Check the constraints of all the rows in the table.
        checkAddConstraintViolations(transaction, table,
                rows, Transaction.INITIALLY_DEFERRED);
    }


    // ---------- Blob store and object management ----------

    /**
     * Creates and allocates storage for a new large object in the blob store.
     * This is called to create a new large object before filling it with data
     * sent from the client.
     */
    Ref createNewLargeObject(byte type, long size) {
        try {
            // If the conglomerate is read-only, a blob can not be created.
            if (isReadOnly()) {
                throw new RuntimeException(
                        "A new large object can not be allocated " +
                                "with a read-only conglomerate");
            }
            // Allocate the large object from the store
            Ref ref = blob_store.allocateLargeObject(type, size);
            // Return the large object reference
            return ref;
        } catch (IOException e) {
            Debug().writeException(e);
            throw new RuntimeException("IO Error when creating blob: " +
                    e.getMessage());
        }
    }

    /**
     * Called when one or more blobs has been completed.  This flushes the blob
     * to the blob store and completes the blob write procedure.  It's important
     * this is called otherwise the BlobStore may not be correctly flushed to
     * disk with the changes and the data will not be recoverable if a crash
     * occurs.
     */
    void flushBlobStore() {
        // NOTE: no longer necessary - please deprecate
    }


    // ---------- Conglomerate diagnosis and repair methods ----------

    /**
     * Checks the conglomerate state file.  The returned ErrorState object
     * contains information about any error generated.
     */
    public void fix(String name, UserTerminal terminal) {
        this.name = name;

        try {

            String state_fn = (name + STATE_POST);
            boolean state_exists = false;
            try {
                state_exists = exists(name);
            } catch (IOException e) {
                terminal.println("IO Error when checking if state store exists: " +
                        e.getMessage());
                e.printStackTrace();
            }

            if (!state_exists) {
                terminal.println("Couldn't find store: " + state_fn);
                return;
            }
            terminal.println("+ Found state store: " + state_fn);

            // Open the state store
            try {
                act_state_store = storeSystem().openStore(name + STATE_POST);
                state_store = new StateStore(act_state_store);
                // Get the 64 byte fixed area
                Area fixed_area = act_state_store.getArea(-1);
                long head_p = fixed_area.getLong();
                state_store.init(head_p);
                terminal.println("+ Initialized the state store: " + state_fn);
            } catch (IOException e) {
                // Couldn't initialize the state file.
                terminal.println("Couldn't initialize the state file: " + state_fn +
                        " Reason: " + e.getMessage());
                return;
            }

            // Initialize the blob store
            try {
                initializeBlobStore();
            } catch (IOException e) {
                terminal.println("Error intializing BlobStore: " + e.getMessage());
                e.printStackTrace();
                return;
            }
            // Setup internal
            setupInternal();

            try {
                checkVisibleTables(terminal);

                // Reset the sequence id's for the system tables
                terminal.println("+ RESETTING ALL SYSTEM TABLE UNIQUE ID VALUES.");
                resetAllSystemTableID();

                // Some diagnostic information
                StringBuffer buf = new StringBuffer();
                MasterTableDataSource t;
                StateResource[] committed_tables = state_store.getVisibleList();
                StateResource[] committed_dropped = state_store.getDeleteList();
                for (int i = 0; i < committed_tables.length; ++i) {
                    terminal.println("+ COMMITTED TABLE: " +
                            committed_tables[i].name);
                }
                for (int i = 0; i < committed_dropped.length; ++i) {
                    terminal.println("+ COMMIT DROPPED TABLE: " +
                            committed_dropped[i].name);
                }

                return;

            } catch (IOException e) {
                terminal.println("IOException: " + e.getMessage());
                e.printStackTrace();
            }

        } finally {
            try {
                close();
            } catch (IOException e) {
                terminal.println("Unable to close conglomerate after fix.");
            }
        }

    }


    // ---------- Conveniences for commit ----------

    /**
     * A static container class for information collected about a table during
     * the commit cycle.
     */
    private static class CommitTableInfo {
        // The master table
        MasterTableDataSource master;
        // The immutable index set
        IndexSet index_set;
        // The journal describing the changes to this table by this
        // transaction.
        MasterTableJournal journal;
        // A list of journals describing changes since this transaction
        // started.
        MasterTableJournal[] changes_since_commit;
        // Break down of changes to the table
        // Normalized list of row ids that were added
        int[] norm_added_rows;
        // Normalized list of row ids that were removed
        int[] norm_removed_rows;
    }

    /**
     * Returns true if the given List of 'CommitTableInfo' objects contains an
     * entry for the given master table.
     */
    private static boolean commitTableListContains(List list,
                                                   MasterTableDataSource master) {
        int sz = list.size();
        for (int i = 0; i < sz; ++i) {
            CommitTableInfo info = (CommitTableInfo) list.get(i);
            if (info.master.equals(master)) {
                return true;
            }
        }
        return false;
    }


    // ---------- low level File IO level operations on a conglomerate ----------
    // These operations are low level IO operations on the contents of the
    // conglomerate.  How the rows and tables are organised is up to the
    // transaction managemenet.  These methods deal with the low level
    // operations of creating/dropping tables and adding, deleting and querying
    // row in tables.

    /**
     * Tries to commit a transaction to the conglomerate.  This is called
     * by the 'closeAndCommit' method in Transaction.  An overview of how this
     * works follows:
     * <ul>
     * <li> Determine if any transactions have been committed since this
     *  transaction was created.
     * <li> If no transactions committed then commit this transaction and exit.
     * <li> Otherwise, determine the tables that have been changed by the
     *  committed transactions since this was created.
     * <li> If no tables changed in the tables changed by this transaction then
     *  commit this transaction and exit.
     * <li> Determine if there are any rows that have been deleted that this
     *  transaction read/deleted.
     * <li> If there are then rollback this transaction and throw an error.
     * <li> Determine if any rows have been added to the tables this transaction
     *  read/changed.
     * <li> If there are then rollback this transaction and throw an error.
     * <li> Otherwise commit the transaction.
     * </ul>
     *
     * @param transaction the transaction to commit from.
     * @param visible_tables the list of visible tables at the end of the commit
     *   (MasterTableDataSource)
     * @param selected_from_tables ths list of tables that this transaction
     *   performed 'select' like queries on (MasterTableDataSource)
     * @param touched_tables the list of tables touched by the transaction
     *   (MutableTableDataSource)
     * @param journal the journal that describes all the changes within the
     *   transaction.
     */
    void processCommit(Transaction transaction, ArrayList visible_tables,
                       ArrayList selected_from_tables,
                       ArrayList touched_tables, TransactionJournal journal)
            throws TransactionException {

        // Get individual journals for updates made to tables in this
        // transaction.
        // The list MasterTableJournal
        ArrayList journal_list = new ArrayList();
        for (int i = 0; i < touched_tables.size(); ++i) {
            MasterTableJournal table_journal =
                    ((MutableTableDataSource) touched_tables.get(i)).getJournal();
            if (table_journal.entries() > 0) {  // Check the journal has entries.
                journal_list.add(table_journal);
            }
        }
        MasterTableJournal[] changed_tables =
                (MasterTableJournal[]) journal_list.toArray(
                        new MasterTableJournal[journal_list.size()]);

        // The list of tables created by this journal.
        IntegerVector created_tables = journal.getTablesCreated();
        // Ths list of tables dropped by this journal.
        IntegerVector dropped_tables = journal.getTablesDropped();
        // The list of tables that constraints were alter by this journal
        IntegerVector constraint_altered_tables =
                journal.getTablesConstraintAltered();

        // Exit early if nothing changed (this is a read-only transaction)
        if (changed_tables.length == 0 &&
                created_tables.size() == 0 && dropped_tables.size() == 0 &&
                constraint_altered_tables.size() == 0) {
            closeTransaction(transaction);
            return;
        }

        // This flag is set to true when entries from the changes tables are
        // at a point of no return.  If this is false it is safe to rollback
        // changes if necessary.
        boolean entries_committed = false;

        // The tables that were actually changed (MasterTableDataSource)
        ArrayList changed_tables_list = new ArrayList();

        // Grab the commit lock.
        synchronized (commit_lock) {

            // Get the list of all database objects that were created in the
            // transaction.
            ArrayList database_objects_created = transaction.getAllNamesCreated();
            // Get the list of all database objects that were dropped in the
            // transaction.
            ArrayList database_objects_dropped = transaction.getAllNamesDropped();

            // This is a transaction that will represent the view of the database
            // at the end of the commit
            Transaction check_transaction = null;

            try {

                // ---- Commit check stage ----

                long tran_commit_id = transaction.getCommitID();

                // We only perform this check if transaction error on dirty selects
                // are enabled.
                if (transaction.transactionErrorOnDirtySelect()) {

                    // For each table that this transaction selected from, if there are
                    // any committed changes then generate a transaction error.
                    for (int i = 0; i < selected_from_tables.size(); ++i) {
                        MasterTableDataSource selected_table =
                                (MasterTableDataSource) selected_from_tables.get(i);
                        // Find all committed journals equal to or greater than this
                        // transaction's commit_id.
                        MasterTableJournal[] journals_since =
                                selected_table.findAllJournalsSince(tran_commit_id);
                        if (journals_since.length > 0) {
                            // Yes, there are changes so generate transaction error and
                            // rollback.
                            throw new TransactionException(
                                    TransactionException.DIRTY_TABLE_SELECT,
                                    "Concurrent Serializable Transaction Conflict(4): " +
                                            "Select from table that has committed changes: " +
                                            selected_table.getName());
                        }
                    }
                }

                // Check there isn't a namespace clash with database objects.
                // We need to create a list of all create and drop activity in the
                // conglomerate from when the transaction started.
                ArrayList all_dropped_obs = new ArrayList();
                ArrayList all_created_obs = new ArrayList();
                int nsj_sz = namespace_journal_list.size();
                for (int i = 0; i < nsj_sz; ++i) {
                    NameSpaceJournal ns_journal =
                            (NameSpaceJournal) namespace_journal_list.get(i);
                    if (ns_journal.commit_id >= tran_commit_id) {
                        all_dropped_obs.addAll(ns_journal.dropped_names);
                        all_created_obs.addAll(ns_journal.created_names);
                    }
                }

                // The list of all dropped objects since this transaction
                // began.
                int ado_sz = all_dropped_obs.size();
                boolean conflict5 = false;
                Object conflict_name = null;
                String conflict_desc = "";
                for (int n = 0; n < ado_sz; ++n) {
                    if (database_objects_dropped.contains(all_dropped_obs.get(n))) {
                        conflict5 = true;
                        conflict_name = all_dropped_obs.get(n);
                        conflict_desc = "Drop Clash";
                    }
                }
                // The list of all created objects since this transaction
                // began.
                int aco_sz = all_created_obs.size();
                for (int n = 0; n < aco_sz; ++n) {
                    if (database_objects_created.contains(all_created_obs.get(n))) {
                        conflict5 = true;
                        conflict_name = all_created_obs.get(n);
                        conflict_desc = "Create Clash";
                    }
                }
                if (conflict5) {
                    // Namespace conflict...
                    throw new TransactionException(
                            TransactionException.DUPLICATE_TABLE,
                            "Concurrent Serializable Transaction Conflict(5): " +
                                    "Namespace conflict: " + conflict_name.toString() + " " +
                                    conflict_desc);
                }

                // For each journal,
                for (int i = 0; i < changed_tables.length; ++i) {
                    MasterTableJournal change_journal = changed_tables[i];
                    // The table the change was made to.
                    int table_id = change_journal.getTableID();
                    // Get the master table with this table id.
                    MasterTableDataSource master = getMasterTable(table_id);

                    // True if the state contains a committed resource with the given name
                    boolean committed_resource =
                            state_store.containsVisibleResource(table_id);

                    // Check this table is still in the committed tables list.
                    if (!created_tables.contains(table_id) &&
                            !committed_resource) {
                        // This table is no longer a committed table, so rollback
                        throw new TransactionException(
                                TransactionException.TABLE_DROPPED,
                                "Concurrent Serializable Transaction Conflict(2): " +
                                        "Table altered/dropped: " + master.getName());
                    }

                    // Since this journal was created, check to see if any changes to the
                    // tables have been committed since.
                    // This will return all journals on the table with the same commit_id
                    // or greater.
                    MasterTableJournal[] journals_since =
                            master.findAllJournalsSince(tran_commit_id);

                    // For each journal, determine if there's any clashes.
                    for (int n = 0; n < journals_since.length; ++n) {
                        // This will thrown an exception if a commit classes.
                        change_journal.testCommitClash(master.getDataTableDef(),
                                journals_since[n]);
                    }

                }

                // Look at the transaction journal, if a table is dropped that has
                // journal entries since the last commit then we have an exception
                // case.
                for (int i = 0; i < dropped_tables.size(); ++i) {
                    int table_id = dropped_tables.intAt(i);
                    // Get the master table with this table id.
                    MasterTableDataSource master = getMasterTable(table_id);
                    // Any journal entries made to this dropped table?
                    if (master.findAllJournalsSince(tran_commit_id).length > 0) {
                        // Oops, yes, rollback!
                        throw new TransactionException(
                                TransactionException.TABLE_REMOVE_CLASH,
                                "Concurrent Serializable Transaction Conflict(3): " +
                                        "Dropped table has modifications: " + master.getName());
                    }
                }

                // Tests passed so go on to commit,

                // ---- Commit stage ----

                // Create a normalized list of MasterTableDataSource of all tables that
                // were either changed (and not dropped), and created (and not dropped).
                // This list represents all tables that are either new or changed in
                // this transaction.

                final int created_tables_count = created_tables.size();
                final int changed_tables_count = changed_tables.length;
                final ArrayList normalized_changed_tables = new ArrayList(8);
                // Add all tables that were changed and not dropped in this transaction.
                for (int i = 0; i < changed_tables_count; ++i) {
                    MasterTableJournal table_journal = changed_tables[i];
                    // The table the changes were made to.
                    int table_id = table_journal.getTableID();
                    // If this table is not dropped in this transaction and is not
                    // already in the normalized list then add it.
                    if (!dropped_tables.contains(table_id)) {
                        MasterTableDataSource master_table = getMasterTable(table_id);

                        CommitTableInfo table_info = new CommitTableInfo();
                        table_info.master = master_table;
                        table_info.journal = table_journal;
                        table_info.changes_since_commit =
                                master_table.findAllJournalsSince(tran_commit_id);

                        normalized_changed_tables.add(table_info);
                    }
                }

                // Add all tables that were created and not dropped in this transaction.
                for (int i = 0; i < created_tables_count; ++i) {
                    int table_id = created_tables.intAt(i);
                    // If this table is not dropped in this transaction then this is a
                    // new table in this transaction.
                    if (!dropped_tables.contains(table_id)) {
                        MasterTableDataSource master_table = getMasterTable(table_id);
                        if (!commitTableListContains(normalized_changed_tables,
                                master_table)) {

                            // This is for entries that are created but modified (no journal).
                            CommitTableInfo table_info = new CommitTableInfo();
                            table_info.master = master_table;

                            normalized_changed_tables.add(table_info);
                        }
                    }
                }

                // The final size of the normalized changed tables list
                final int norm_changed_tables_count = normalized_changed_tables.size();

                // Create a normalized list of MasterTableDataSource of all tables that
                // were dropped (and not created) in this transaction.  This list
                // represents tables that will be dropped if the transaction
                // successfully commits.

                final int dropped_tables_count = dropped_tables.size();
                final ArrayList normalized_dropped_tables = new ArrayList(8);
                for (int i = 0; i < dropped_tables_count; ++i) {
                    // The dropped table
                    int table_id = dropped_tables.intAt(i);
                    // Was this dropped table also created?  If it was created in this
                    // transaction then we don't care about it.
                    if (!created_tables.contains(table_id)) {
                        MasterTableDataSource master_table = getMasterTable(table_id);
                        normalized_dropped_tables.add(master_table);
                    }
                }

                // We now need to create a SimpleTransaction object that we
                // use to send to the triggering mechanism.  This
                // SimpleTransaction represents a very specific view of the
                // transaction.  This view contains the latest version of changed
                // tables in this transaction.  It also contains any tables that have
                // been created by this transaction and does not contain any tables
                // that have been dropped.  Any tables that have not been touched by
                // this transaction are shown in their current committed state.
                // To summarize - this view is the current view of the database plus
                // any modifications made by the transaction that is being committed.

                // How this works - All changed tables are merged with the current
                // committed table.  All created tables are added into check_transaction
                // and all dropped tables are removed from check_transaction.  If
                // there were no other changes to a table between the time the
                // transaction was created and now, the view of the table in the
                // transaction is used, otherwise the latest changes are merged.

                // Note that this view will be the view that the database will
                // ultimately become if this transaction successfully commits.  Also,
                // you should appreciate that this view is NOT exactly the same as
                // the current trasaction view because any changes that have been
                // committed by concurrent transactions will be reflected in this view.

                // Create a new transaction of the database which will represent the
                // committed view if this commit is successful.
                check_transaction = createTransaction();

                // Overwrite this view with tables from this transaction that have
                // changed or have been added or dropped.

                // (Note that order here is important).  First drop any tables from
                // this view.
                for (int i = 0; i < normalized_dropped_tables.size(); ++i) {
                    // Get the table
                    MasterTableDataSource master_table =
                            (MasterTableDataSource) normalized_dropped_tables.get(i);
                    // Drop this table in the current view
                    check_transaction.removeVisibleTable(master_table);
                }

                // Now add any changed tables to the view.

                // Represents view of the changed tables
                TableDataSource[] changed_table_source =
                        new TableDataSource[norm_changed_tables_count];
                // Set up the above arrays
                for (int i = 0; i < norm_changed_tables_count; ++i) {

                    // Get the information for this changed table
                    CommitTableInfo table_info =
                            (CommitTableInfo) normalized_changed_tables.get(i);

                    // Get the master table that changed from the normalized list.
                    MasterTableDataSource master = table_info.master;
                    // Did this table change since the transaction started?
                    MasterTableJournal[] all_table_changes =
                            table_info.changes_since_commit;

                    if (all_table_changes == null || all_table_changes.length == 0) {
                        // No changes so we can pick the correct IndexSet from the current
                        // transaction.

                        // Get the state of the changed tables from the Transaction
                        MutableTableDataSource mtable =
                                transaction.getTable(master.getTableName());
                        // Get the current index set of the changed table
                        table_info.index_set = transaction.getIndexSetForTable(master);
                        // Flush all index changes in the table
                        mtable.flushIndexChanges();

                        // Set the 'check_transaction' object with the latest version of the
                        // table.
                        check_transaction.updateVisibleTable(table_info.master,
                                table_info.index_set);

                    } else {
                        // There were changes so we need to merge the changes with the
                        // current view of the table.

                        // It's not immediately obvious how this merge update works, but
                        // basically what happens is we put the table journal with all the
                        // changes into a new MutableTableDataSource of the current
                        // committed state, and then we flush all the changes into the
                        // index and then update the 'check_transaction' with this change.

                        // Create the MutableTableDataSource with the changes from this
                        // journal.
                        MutableTableDataSource mtable =
                                master.createTableDataSourceAtCommit(check_transaction,
                                        table_info.journal);
                        // Get the current index set of the changed table
                        table_info.index_set =
                                check_transaction.getIndexSetForTable(master);
                        // Flush all index changes in the table
                        mtable.flushIndexChanges();

                        // Dispose the table
                        mtable.dispose();

                    }

                    // And now refresh the 'changed_table_source' entry
                    changed_table_source[i] =
                            check_transaction.getTable(master.getTableName());

                }

                // The 'check_transaction' now represents the view the database will be
                // if the commit succeeds.  We lock 'check_transaction' so it is
                // read-only (the view is immutable).
                check_transaction.setReadOnly();

                // Any tables that the constraints were altered for we need to check
                // if any rows in the table violate the new constraints.
                for (int i = 0; i < constraint_altered_tables.size(); ++i) {
                    // We need to check there are no constraint violations for all the
                    // rows in the table.
                    int table_id = constraint_altered_tables.intAt(i);
                    for (int n = 0; n < norm_changed_tables_count; ++n) {
                        CommitTableInfo table_info =
                                (CommitTableInfo) normalized_changed_tables.get(n);
                        if (table_info.master.getTableID() == table_id) {
                            checkAllAddConstraintViolations(check_transaction,
                                    changed_table_source[n],
                                    Transaction.INITIALLY_DEFERRED);
                        }
                    }
                }

                // For each changed table we must determine the rows that
                // were deleted and perform the remove constraint checks on the
                // deleted rows.  Note that this happens after the records are
                // removed from the index.

                // For each changed table,
                for (int i = 0; i < norm_changed_tables_count; ++i) {
                    CommitTableInfo table_info =
                            (CommitTableInfo) normalized_changed_tables.get(i);
                    // Get the journal that details the change to the table.
                    MasterTableJournal change_journal = table_info.journal;
                    if (change_journal != null) {
                        // Find the normalized deleted rows.
                        int[] normalized_removed_rows =
                                change_journal.normalizedRemovedRows();
                        // Check removing any of the data doesn't cause a constraint
                        // violation.
                        checkRemoveConstraintViolations(check_transaction,
                                changed_table_source[i], normalized_removed_rows,
                                Transaction.INITIALLY_DEFERRED);

                        // Find the normalized added rows.
                        int[] normalized_added_rows =
                                change_journal.normalizedAddedRows();
                        // Check adding any of the data doesn't cause a constraint
                        // violation.
                        checkAddConstraintViolations(check_transaction,
                                changed_table_source[i], normalized_added_rows,
                                Transaction.INITIALLY_DEFERRED);

                        // Set up the list of added and removed rows
                        table_info.norm_added_rows = normalized_added_rows;
                        table_info.norm_removed_rows = normalized_removed_rows;

                    }
                }

                // Deferred trigger events.
                // For each changed table.
                n_loop:
                for (int i = 0; i < norm_changed_tables_count; ++i) {
                    CommitTableInfo table_info =
                            (CommitTableInfo) normalized_changed_tables.get(i);
                    // Get the journal that details the change to the table.
                    MasterTableJournal change_journal = table_info.journal;
                    if (change_journal != null) {
                        // Get the table name
                        TableName table_name = table_info.master.getTableName();
                        // The list of listeners to dispatch this event to
                        TransactionModificationListener[] listeners;
                        // Are there any listeners listening for events on this table?
                        synchronized (modification_listeners) {
                            ArrayList list =
                                    (ArrayList) modification_listeners.get(table_name);
                            if (list == null || list.size() == 0) {
                                // If no listeners on this table, continue to the next
                                // table that was changed.
                                continue n_loop;
                            }
                            // Generate the list of listeners,
                            listeners = (TransactionModificationListener[]) list.toArray(
                                    new TransactionModificationListener[list.size()]);
                        }
                        // Generate the event
                        TableCommitModificationEvent event =
                                new TableCommitModificationEvent(
                                        check_transaction, table_name,
                                        table_info.norm_added_rows,
                                        table_info.norm_removed_rows);
                        // Fire this event on the listeners
                        for (int n = 0; n < listeners.length; ++n) {
                            listeners[n].tableCommitChange(event);
                        }

                    }  // if (change_journal != null)
                }  // for each changed table

                // NOTE: This isn't as fail safe as it could be.  We really need to
                //  do the commit in two phases.  The first writes updated indices to
                //  the index files.  The second updates the header pointer for the
                //  respective table.  Perhaps we can make the header update
                //  procedure just one file write.

                // Finally, at this point all constraint checks have passed and the
                // changes are ready to finally be committed as permanent changes
                // to the conglomerate.  All that needs to be done is to commit our
                // IndexSet indices for each changed table as final.
                // ISSUE: Should we separate the 'committing of indexes' changes and
                //   'committing of delete/add flags' to make the FS more robust?
                //   It would be more robust if all indexes are committed in one go,
                //   then all table flag data.

                // Set flag to indicate we have committed entries.
                entries_committed = true;

                // For each change to each table,
                for (int i = 0; i < norm_changed_tables_count; ++i) {
                    CommitTableInfo table_info =
                            (CommitTableInfo) normalized_changed_tables.get(i);
                    // Get the journal that details the change to the table.
                    MasterTableJournal change_journal = table_info.journal;
                    if (change_journal != null) {
                        // Get the master table with this table id.
                        MasterTableDataSource master = table_info.master;
                        // Commit the changes to the table.
                        // We use 'this.commit_id' which is the current commit level we are
                        // at.
                        master.commitTransactionChange(this.commit_id, change_journal,
                                table_info.index_set);
                        // Add to 'changed_tables_list'
                        changed_tables_list.add(master);
                    }
                }

                // Only do this if we've created or dropped tables.
                if (created_tables.size() > 0 || dropped_tables.size() > 0) {
                    // Update the committed tables in the conglomerate state.
                    // This will update and synchronize the headers in this conglomerate.
                    commitToTables(created_tables, dropped_tables);
                }

                // Update the namespace clash list
                if (database_objects_created.size() > 0 ||
                        database_objects_dropped.size() > 0) {
                    NameSpaceJournal namespace_journal =
                            new NameSpaceJournal(tran_commit_id,
                                    database_objects_created,
                                    database_objects_dropped);
                    namespace_journal_list.add(namespace_journal);
                }

            } finally {

                try {

                    // If entries_committed == false it means we didn't get to a point
                    // where any changed tables were committed.  Attempt to rollback the
                    // changes in this transaction if they haven't been committed yet.
                    if (entries_committed == false) {
                        // For each change to each table,
                        for (int i = 0; i < changed_tables.length; ++i) {
                            // Get the journal that details the change to the table.
                            MasterTableJournal change_journal = changed_tables[i];
                            // The table the changes were made to.
                            int table_id = change_journal.getTableID();
                            // Get the master table with this table id.
                            MasterTableDataSource master = getMasterTable(table_id);
                            // Commit the rollback on the table.
                            master.rollbackTransactionChange(change_journal);
                        }
                        if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                            Debug().write(Lvl.INFORMATION, this,
                                    "Rolled back transaction changes in a commit.");
                        }
                    }

                } finally {
                    try {
                        // Dispose the 'check_transaction'
                        if (check_transaction != null) {
                            check_transaction.dispose();
                            closeTransaction(check_transaction);
                        }
                        // Always ensure a transaction close, even if we have an exception.
                        // Notify the conglomerate that this transaction has closed.
                        closeTransaction(transaction);
                    } catch (Throwable e) {
                        Debug().writeException(e);
                    }
                }

            }

            // Flush the journals up to the minimum commit id for all the tables
            // that this transaction changed.
            long min_commit_id = open_transactions.minimumCommitID(null);
            int chsz = changed_tables_list.size();
            for (int i = 0; i < chsz; ++i) {
                MasterTableDataSource master =
                        (MasterTableDataSource) changed_tables_list.get(i);
                master.mergeJournalChanges(min_commit_id);
            }
            int nsjsz = namespace_journal_list.size();
            for (int i = nsjsz - 1; i >= 0; --i) {
                NameSpaceJournal namespace_journal =
                        (NameSpaceJournal) namespace_journal_list.get(i);
                // Remove if the commit id for the journal is less than the minimum
                // commit id
                if (namespace_journal.commit_id < min_commit_id) {
                    namespace_journal_list.remove(i);
                }
            }

            // Set a check point in the store system.  This means that the
            // persistance state is now stable.
            store_system.setCheckPoint();

        }  // synchronized (commit_lock)

    }

    /**
     * Rollbacks a transaction and invalidates any changes that the transaction
     * made to the database.  The rows that this transaction changed are given
     * up as freely available rows.  This is called by the 'closeAndRollback'
     * method in Transaction.
     */
    void processRollback(Transaction transaction,
                         ArrayList touched_tables, TransactionJournal journal) {

        // Go through the journal.  Any rows added should be marked as deleted
        // in the respective master table.

        // Get individual journals for updates made to tables in this
        // transaction.
        // The list MasterTableJournal
        ArrayList journal_list = new ArrayList();
        for (int i = 0; i < touched_tables.size(); ++i) {
            MasterTableJournal table_journal =
                    ((MutableTableDataSource) touched_tables.get(i)).getJournal();
            if (table_journal.entries() > 0) {  // Check the journal has entries.
                journal_list.add(table_journal);
            }
        }
        MasterTableJournal[] changed_tables =
                (MasterTableJournal[]) journal_list.toArray(
                        new MasterTableJournal[journal_list.size()]);

        // The list of tables created by this journal.
        IntegerVector created_tables = journal.getTablesCreated();

        synchronized (commit_lock) {

            try {

                // For each change to each table,
                for (int i = 0; i < changed_tables.length; ++i) {
                    // Get the journal that details the change to the table.
                    MasterTableJournal change_journal = changed_tables[i];
                    // The table the changes were made to.
                    int table_id = change_journal.getTableID();
                    // Get the master table with this table id.
                    MasterTableDataSource master = getMasterTable(table_id);
                    // Commit the rollback on the table.
                    master.rollbackTransactionChange(change_journal);
                }

            } finally {
                // Notify the conglomerate that this transaction has closed.
                closeTransaction(transaction);
            }
        }
    }

    // -----

    /**
     * Sets the given List of MasterTableDataSource objects to the currently
     * committed list of tables in this conglomerate.  This will make the change
     * permanent by updating the state file also.
     * <p>
     * This should be called as part of a transaction commit.
     */
    private void commitToTables(
            IntegerVector created_tables, IntegerVector dropped_tables) {

        // Add created tables to the committed tables list.
        for (int i = 0; i < created_tables.size(); ++i) {
            // For all created tables, add to the visible list and remove from the
            // delete list in the state store.
            MasterTableDataSource t = getMasterTable(created_tables.intAt(i));
            StateResource resource =
                    new StateResource(t.getTableID(), createEncodedTableFile(t));
            state_store.addVisibleResource(resource);
            state_store.removeDeleteResource(resource.name);
        }

        // Remove dropped tables from the committed tables list.
        for (int i = 0; i < dropped_tables.size(); ++i) {
            // For all dropped tables, add to the delete list and remove from the
            // visible list in the state store.
            MasterTableDataSource t = getMasterTable(dropped_tables.intAt(i));
            StateResource resource =
                    new StateResource(t.getTableID(), createEncodedTableFile(t));
            state_store.addDeleteResource(resource);
            state_store.removeVisibleResource(resource.name);
        }

        try {
            state_store.commit();
        } catch (IOException e) {
            Debug().writeException(e);
            throw new Error("IO Error: " + e.getMessage());
        }
    }

    /**
     * Returns the MasterTableDataSource in this conglomerate with the given
     * table id.
     */
    MasterTableDataSource getMasterTable(int table_id) {
        synchronized (commit_lock) {
            // Find the table with this table id.
            for (int i = 0; i < table_list.size(); ++i) {
                MasterTableDataSource t = (MasterTableDataSource) table_list.get(i);
                if (t.getTableID() == table_id) {
                    return t;
                }
            }
            throw new Error("Unable to find an open table with id: " + table_id);
        }
    }

    /**
     * Creates a table store in this conglomerate with the given name and returns
     * a reference to the table.  Note that this table is not a commited change
     * to the system.  It is a free standing blank table store.  The table
     * returned here is uncommitted and will be deleted unless it is committed.
     * <p>
     * Note that two tables may exist within a conglomerate with the same name,
     * however each <b>committed</b> table must have a unique name.
     * <p>
     * @param table_def the table definition.
     * @param data_sector_size the size of the data sectors (affects performance
     *   and size of the file).
     * @param index_sector_size the size of the index sectors.
     */
    MasterTableDataSource createMasterTable(DataTableDef table_def,
                                            int data_sector_size, int index_sector_size) {
        synchronized (commit_lock) {
            try {

                // EFFICIENCY: Currently this writes to the conglomerate state file
                //   twice.  Once in 'nextUniqueTableID' and once in
                //   'state_store.commit'.

                // The unique id that identifies this table,
                int table_id = nextUniqueTableID();

                // Create the object.
                V2MasterTableDataSource master_table =
                        new V2MasterTableDataSource(getSystem(),
                                storeSystem(), open_transactions, blob_store);
                master_table.create(table_id, table_def);

                // Add to the list of all tables.
                table_list.add(master_table);

                // Add this to the list of deleted tables,
                // (This should really be renamed to uncommitted tables).
                markAsCommittedDropped(table_id);

                // Commit this
                state_store.commit();

                // And return it.
                return master_table;

            } catch (IOException e) {
                Debug().writeException(e);
                throw new Error("Unable to create master table '" +
                        table_def.getName() + "' - " + e.getMessage());
            }
        }

    }

    /**
     * Creates a table store in this conglomerate that is an exact copy of the
     * given MasterTableDataSource.  Note that this table is not a commited change
     * to the system.  It is a free standing blank table store.  The table
     * returned here is uncommitted and will be deleted unless it is committed.
     * <p>
     * Note that two tables may exist within a conglomerate with the same name,
     * however each <b>committed</b> table must have a unique name.
     * <p>
     * @param src_master_table the source master table to copy.
     * @param index_set the view of the table index to copy.
     * @return the MasterTableDataSource with the copied information.
     */
    MasterTableDataSource copyMasterTable(
            MasterTableDataSource src_master_table, IndexSet index_set) {
        synchronized (commit_lock) {
            try {

                // EFFICIENCY: Currently this writes to the conglomerate state file
                //   twice.  Once in 'nextUniqueTableID' and once in
                //   'state_store.commit'.

                // The unique id that identifies this table,
                int table_id = nextUniqueTableID();

                // Create the object.
                V2MasterTableDataSource master_table =
                        new V2MasterTableDataSource(getSystem(),
                                storeSystem(), open_transactions, blob_store);

                master_table.copy(table_id, src_master_table, index_set);

                // Add to the list of all tables.
                table_list.add(master_table);

                // Add this to the list of deleted tables,
                // (This should really be renamed to uncommitted tables).
                markAsCommittedDropped(table_id);

                // Commit this
                state_store.commit();

                // And return it.
                return master_table;

            } catch (IOException e) {
                Debug().writeException(e);
                throw new RuntimeException("Unable to copy master table '" +
                        src_master_table.getDataTableDef().getName() +
                        "' - " + e.getMessage());
            }
        }

    }

    // ---------- Inner classes ----------

    /**
     * A journal for handling namespace clashes between transactions.  For
     * example, we would need to generate a conflict if two concurrent
     * transactions were to drop the same table, or if a procedure and a
     * table with the same name were generated in concurrent transactions.
     */
    private static class NameSpaceJournal {

        /**
         * The commit_id of this journal entry.
         */
        long commit_id;

        /**
         * The list of names created in this journal.
         */
        ArrayList created_names;

        /**
         * The list of names dropped in this journal.
         */
        ArrayList dropped_names;

        /**
         * Constructs the journal.
         */
        NameSpaceJournal(long commit_id,
                         ArrayList created_names, ArrayList dropped_names) {
            this.commit_id = commit_id;
            this.created_names = created_names;
            this.dropped_names = dropped_names;
        }

    }


//  // ---------- Shutdown hook ----------
//  
//  /**
//   * This is a thread that is started when the shutdown hook for this
//   * conglomerate is executed.  It goes through each table in the conglomerate
//   * and attempts to lock the 'writeLockedObject' for each table.  When all the
//   * objects are locked it goes into a wait state.
//   */
//  private class ConglomerateShutdownHookThread extends Thread {
//    private boolean complete = false;
//    
//    ConglomerateShutdownHookThread() {
//      setName("Pony - JVM Shutdown Hook");
//    }
//    
//    public synchronized void run() {
//      // Synchronize over the commit_lock object
//      synchronized (commit_lock) {
//        if (table_list != null) {
////          System.out.println("Cleanup on: " + TableDataConglomerate.this);
//          for (int i = 0; i < table_list.size(); ++i) {
//            MasterTableDataSource master =
//                                   (MasterTableDataSource) table_list.get(i);
////            System.out.println("CLEANUP: " + master);
//            master.shutdownHookCleanup();
//          }
//        }
//      }
//      complete = true;
//      notifyAll();
//    }
//    public synchronized void waitUntilComplete() {
//      try {
//        while (!complete) {
//          wait();
//        }
//      }
//      catch (InterruptedException e) { /* ignore */ }
//    }
//  }

    public void finalize() {
//    removeShutdownHook();
    }


}
