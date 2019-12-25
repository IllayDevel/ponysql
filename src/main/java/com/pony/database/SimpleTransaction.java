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

import com.pony.debug.DebugLogger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * An simple implementation of Transaction that provides various facilities for
 * implementing a Transaction object on a number of MasterTableDataSource
 * tables.  The Transaction object is designed such that concurrent
 * modification can happen to the database via other transactions without this
 * view of the database being changed.
 * <p>
 * This object does not implement any transaction control mechanisms such as
 * 'commit' or 'rollback'.  This object is most useful for setting up a
 * short-term minimal transaction for modifying or querying some data in the
 * database given on some view.
 *
 * @author Tobias Downer
 */

public abstract class SimpleTransaction {

    /**
     * The TransactionSystem context.
     */
    private final TransactionSystem system;

    /**
     * The list of tables that represent this transaction's view of the database.
     * (MasterTableDataSource).
     */
    private final ArrayList visible_tables;

    /**
     * An IndexSet for each visible table from the above list.  These objects
     * are used to represent index information for all tables.
     * (IndexSet)
     */
    private final ArrayList table_indices;

    /**
     * A queue of MasterTableDataSource and IndexSet objects that are pending to
     * be cleaned up when this transaction is disposed.
     */
    private ArrayList cleanup_queue;

    /**
     * A cache of tables that have been accessed via this transaction.  This is
     * a map of table_name -> MutableTableDataSource.
     */
    private final HashMap table_cache;

    /**
     * A local cache for sequence values.
     */
    private final HashMap sequence_value_cache;

    /**
     * The SequenceManager for this abstract transaction.
     */
    private final SequenceManager sequence_manager;

    /**
     * If true, this is a read-only transaction and does not permit any type of
     * modification to this vew of the database.
     */
    private boolean read_only;


    /**
     * Constructs the AbstractTransaction.  SequenceManager may be null in which
     * case sequence generator operations are not permitted.
     */
    SimpleTransaction(TransactionSystem system,
                      SequenceManager sequence_manager) {
        this.system = system;

        this.visible_tables = new ArrayList();
        this.table_indices = new ArrayList();
        this.table_cache = new HashMap();
        this.sequence_value_cache = new HashMap();

        this.sequence_manager = sequence_manager;

        this.read_only = false;
    }

    /**
     * Sets this transaction as read only.  A read only transaction does not
     * allow for the view to be modified in any way.
     */
    public void setReadOnly() {
        read_only = true;
    }

    /**
     * Returns true if the transaction is read-only, otherwise returns false.
     */
    public boolean isReadOnly() {
        return read_only;
    }

    /**
     * Returns the TransactionSystem that this Transaction is part of.
     */
    public final TransactionSystem getSystem() {
        return system;
    }

    /**
     * Returns a list of all visible tables.
     */
    protected final ArrayList getVisibleTables() {
        return visible_tables;
    }

    /**
     * Returns a DebugLogger object that we use to log debug messages to.
     */
    public final DebugLogger Debug() {
        return getSystem().Debug();
    }

    /**
     * Returns the number of visible tables being managed by this transaction.
     */
    protected int getVisibleTableCount() {
        return visible_tables.size();
    }

    /**
     * Returns a MasterTableDataSource object representing table 'n' in the set
     * of tables visible in this transaction.
     */
    protected MasterTableDataSource getVisibleTable(int n) {
        return (MasterTableDataSource) visible_tables.get(n);
    }

    /**
     * Searches through the list of tables visible within this transaction and
     * returns the MasterTableDataSource object with the given name.  Returns
     * null if no visible table with the given name could be found.
     */
    protected MasterTableDataSource findVisibleTable(TableName table_name,
                                                     boolean ignore_case) {

        int size = visible_tables.size();
        for (Object visible_table : visible_tables) {
            MasterTableDataSource master =
                    (MasterTableDataSource) visible_table;
            DataTableDef table_def = master.getDataTableDef();
            if (ignore_case) {
                if (table_def.getTableName().equalsIgnoreCase(table_name)) {
                    return master;
                }
            } else {
                // Not ignore case
                if (table_def.getTableName().equals(table_name)) {
                    return master;
                }
            }
        }
        return null;
    }

    /**
     * Returns the IndexSet for the given MasterTableDataSource object that
     * is visible in this transaction.
     */
    final IndexSet getIndexSetForTable(MasterTableDataSource table) {
        int sz = table_indices.size();
        for (int i = 0; i < sz; ++i) {
            if (visible_tables.get(i) == table) {
                return (IndexSet) table_indices.get(i);
            }
        }
        throw new RuntimeException(
                "MasterTableDataSource not found in this transaction.");
    }

    /**
     * Sets the IndexSet for the given MasterTableDataSource object in this
     * transaction.
     */
    protected final void setIndexSetForTable(MasterTableDataSource table,
                                             IndexSet index_set) {
        int sz = table_indices.size();
        for (int i = 0; i < sz; ++i) {
            if (visible_tables.get(i) == table) {
                table_indices.set(i, index_set);
                return;
            }
        }
        throw new RuntimeException(
                "MasterTableDataSource not found in this transaction.");
    }

    /**
     * Returns true if the given table name is a dynamically generated table and
     * is not a table that is found in the table list defined in this transaction
     * object.
     * <p>
     * It is intended this is implemented by derived classes to handle dynamically
     * generated tables (tables based on some function or from an external data
     * source)
     */
    protected boolean isDynamicTable(TableName table_name) {
        // By default, dynamic tables are not implemented.
        return false;
    }

    /**
     * If this transaction implementation defines dynamic tables (tables whose
     * content is determined by some function), this should return the
     * table here as a MutableTableDataSource object.  If the table is not
     * defined an exception is generated.
     * <p>
     * It is intended this is implemented by derived classes to handle dynamically
     * generated tables (tables based on some function or from an external data
     * source)
     */
    protected MutableTableDataSource getDynamicTable(TableName table_name) {
        // By default, dynamic tables are not implemented.
        throw new StatementException("Table '" + table_name + "' not found.");
    }

    /**
     * Returns the DataTableDef for a dynamic table defined in this transaction.
     * <p>
     * It is intended this is implemented by derived classes to handle dynamically
     * generated tables (tables based on some function or from an external data
     * source)
     */
    protected DataTableDef getDynamicDataTableDef(TableName table_name) {
        // By default, dynamic tables are not implemented.
        throw new StatementException("Table '" + table_name + "' not found.");
    }

    /**
     * Returns a string type describing the type of the dynamic table.
     * <p>
     * It is intended this is implemented by derived classes to handle dynamically
     * generated tables (tables based on some function or from an external data
     * source)
     */
    protected String getDynamicTableType(TableName table_name) {
        // By default, dynamic tables are not implemented.
        throw new StatementException("Table '" + table_name + "' not found.");
    }

    /**
     * Returns a list of all dynamic table names.  We can assume that the object
     * returned here is static so the content of this list should not be changed.
     * <p>
     * It is intended this is implemented by derived classes to handle dynamically
     * generated tables (tables based on some function or from an external data
     * source)
     */
    protected TableName[] getDynamicTableList() {
        return new TableName[0];
    }

    // -----

    /**
     * Returns a new MutableTableDataSource for the view of the
     * MasterTableDataSource at the start of this transaction.  Note that this is
     * called only once per table accessed in this transaction.
     */
    abstract MutableTableDataSource createMutableTableDataSourceAtCommit(
            MasterTableDataSource master);

    // -----

    /**
     * Flushes the table cache and purges the cache of the entry for the given
     * table name.
     */
    protected void flushTableCache(TableName table_name) {
        table_cache.remove(table_name);
    }

    /**
     * Adds a MasterTableDataSource and IndexSet to this transaction view.
     */
    void addVisibleTable(MasterTableDataSource table,
                         IndexSet index_set) {
        if (isReadOnly()) {
            throw new RuntimeException("Transaction is read-only.");
        }

        visible_tables.add(table);
        table_indices.add(index_set);
    }

    /**
     * Removes a MasterTableDataSource (and its IndexSet) from this view and
     * puts the information on the cleanup queue.
     */
    void removeVisibleTable(MasterTableDataSource table) {
        if (isReadOnly()) {
            throw new RuntimeException("Transaction is read-only.");
        }

        int i = visible_tables.indexOf(table);
        if (i != -1) {
            visible_tables.remove(i);
            IndexSet index_set = (IndexSet) table_indices.remove(i);
            if (cleanup_queue == null) {
                cleanup_queue = new ArrayList();
            }
            cleanup_queue.add(table);
            cleanup_queue.add(index_set);
            // Remove from the table cache
            TableName table_name = table.getTableName();
            table_cache.remove(table_name);
        }
    }

    /**
     * Updates a MastertableDataSource (and its IndexSet) for this view.  The
     * existing IndexSet/MasterTableDataSource for this is put on the clean up
     * queue.
     */
    void updateVisibleTable(MasterTableDataSource table,
                            IndexSet index_set) {
        if (isReadOnly()) {
            throw new RuntimeException("Transaction is read-only.");
        }

        removeVisibleTable(table);
        addVisibleTable(table, index_set);
    }

    /**
     * Disposes of all IndexSet objects currently accessed by this Transaction.
     * This includes IndexSet objects on tables that have been dropped by
     * operations on this transaction and are in the 'cleanup_queue' object.
     * Disposing of the IndexSet is a common cleanup practice and would typically
     * be used at the end of a transaction.
     */
    protected void disposeAllIndices() {
        // Dispose all the IndexSet for each table
        try {
            for (Object table_index : table_indices) {
                ((IndexSet) table_index).dispose();
            }
        } catch (Throwable e) {
            Debug().writeException(e);
        }

        // Dispose all tables we dropped (they will be in the cleanup_queue.
        try {
            if (cleanup_queue != null) {
                for (int i = 0; i < cleanup_queue.size(); i += 2) {
                    MasterTableDataSource master =
                            (MasterTableDataSource) cleanup_queue.get(i);
                    IndexSet index_set = (IndexSet) cleanup_queue.get(i + 1);
                    index_set.dispose();
                }
                cleanup_queue = null;
            }
        } catch (Throwable e) {
            Debug().writeException(e);
        }

    }


    // -----

    /**
     * Returns a TableDataSource object that represents the table with the
     * given name within this transaction.  This table is represented by an
     * immutable interface.
     */
    public TableDataSource getTableDataSource(TableName table_name) {
        return getTable(table_name);
    }

    /**
     * Returns a MutableTableDataSource object that represents the table with
     * the given name within this transaction.  Any changes made to this table
     * are only made within the context of this transaction.  This means if a
     * row is added or removed, it is not made perminant until the transaction
     * is committed.
     * <p>
     * If the table does not exist then an exception is thrown.
     */
    public MutableTableDataSource getTable(TableName table_name) {

        // If table is in the cache, return it
        MutableTableDataSource table =
                (MutableTableDataSource) table_cache.get(table_name);
        if (table != null) {
            return table;
        }

        // Is it represented as a master table?
        MasterTableDataSource master = findVisibleTable(table_name, false);

        // Not a master table, so see if it's a dynamic table instead,
        if (master == null) {
            // Is this a dynamic table?
            if (isDynamicTable(table_name)) {
                return getDynamicTable(table_name);
            }
        } else {
            // Otherwise make a view of tha master table data source and put it in
            // the cache.
            table = createMutableTableDataSourceAtCommit(master);

            // Put table name in the cache
            table_cache.put(table_name, table);
        }

        return table;

    }

    /**
     * Returns the DataTableDef for the table with the given name that is
     * visible within this transaction.
     * <p>
     * Returns null if table name doesn't refer to a table that exists.
     */
    public DataTableDef getDataTableDef(TableName table_name) {
        // If this is a dynamic table then handle specially
        if (isDynamicTable(table_name)) {
            return getDynamicDataTableDef(table_name);
        } else {
            // Otherwise return from the pool of visible tables
            int sz = visible_tables.size();
            for (Object visible_table : visible_tables) {
                MasterTableDataSource master =
                        (MasterTableDataSource) visible_table;
                DataTableDef table_def = master.getDataTableDef();
                if (table_def.getTableName().equals(table_name)) {
                    return table_def;
                }
            }
            return null;
        }
    }

    /**
     * Returns a list of table names that are visible within this transaction.
     */
    public TableName[] getTableList() {
        TableName[] internal_tables = getDynamicTableList();

        int sz = visible_tables.size();
        // The result list
        TableName[] tables = new TableName[sz + internal_tables.length];
        // Add the master tables
        for (int i = 0; i < sz; ++i) {
            MasterTableDataSource master =
                    (MasterTableDataSource) visible_tables.get(i);
            DataTableDef table_def = master.getDataTableDef();
            tables[i] = new TableName(table_def.getSchema(), table_def.getName());
        }

        // Add any internal system tables to the list
        for (int i = 0; i < internal_tables.length; ++i) {
            tables[sz + i] = internal_tables[i];
        }

        return tables;
    }

    /**
     * Returns true if the database table object with the given name exists
     * within this transaction.
     */
    public boolean tableExists(TableName table_name) {
//    // NASTY HACK: This hack is to get around an annoying recursive problem
//    //   when resolving views.  We know this table can't possibly be an
//    //   internal table.
//    boolean is_view_table = (table_name.getName().equals("View") &&
//                             table_name.getSchema().equals("SYS_INFO"));
//    if (is_view_table) {
//      return findVisibleTable(table_name, false) != null;
//    }
//    
        return isDynamicTable(table_name) ||
                realTableExists(table_name);
    }

    /**
     * Returns true if the table with the given name exists within this
     * transaction.  This is different from 'tableExists' because it does not try
     * to resolve against dynamic tables, and is therefore useful for quickly
     * checking if a system table exists or not.
     */
    final boolean realTableExists(TableName table_name) {
        return findVisibleTable(table_name, false) != null;
    }

    /**
     * Attempts to resolve the given table name to its correct case assuming
     * the table name represents a case insensitive version of the name.  For
     * example, "aPP.CuSTOMer" may resolve to "APP.Customer".  If the table
     * name can not resolve to a valid identifier it returns the input table
     * name, therefore the actual presence of the table should always be
     * checked by calling 'tableExists' after this method returns.
     */
    public TableName tryResolveCase(TableName table_name) {
        // Is it a visable table (match case insensitive)
        MasterTableDataSource table = findVisibleTable(table_name, true);
        if (table != null) {
            return table.getTableName();
        }
        // Is it an internal table?
        String tschema = table_name.getSchema();
        String tname = table_name.getName();
        TableName[] list = getDynamicTableList();
        for (TableName ctable : list) {
            if (ctable.getSchema().equalsIgnoreCase(tschema) &&
                    ctable.getName().equalsIgnoreCase(tname)) {
                return ctable;
            }
        }

        // No matches so return the original object.
        return table_name;
    }

    /**
     * Returns the type of the table object with the given name.  If the table
     * is a base table, this method returns "TABLE".  If it is a virtual table,
     * it returns the type assigned to by the InternalTableInfo interface.
     */
    public String getTableType(TableName table_name) {
        if (isDynamicTable(table_name)) {
            return getDynamicTableType(table_name);
        } else if (findVisibleTable(table_name, false) != null) {
            return "TABLE";
        }
        // No table found so report the error.
        throw new RuntimeException("No table '" + table_name +
                "' to report type for.");
    }

    /**
     * Resolves the given string to a table name, throwing an exception if
     * the reference is ambiguous.  This also generates an exception if the
     * table object is not found.
     */
    public TableName resolveToTableName(String current_schema,
                                        String name, boolean case_insensitive) {
        TableName table_name = TableName.resolve(current_schema, name);
        TableName[] tables = getTableList();
        TableName found = null;

        for (TableName table : tables) {
            boolean match;
            if (case_insensitive) {
                match = table.equalsIgnoreCase(table_name);
            } else {
                match = table.equals(table_name);
            }
            if (match) {
                if (found != null) {
                    throw new StatementException("Ambiguous reference: " + name);
                } else {
                    found = table;
                }
            }
        }

        if (found == null) {
            throw new StatementException("Object not found: " + name);
        }

        return found;
    }

    // ---------- Sequence management ----------

    /**
     * Flushes the sequence cache.  This should be used whenever a sequence
     * is changed.
     */
    void flushSequenceManager(TableName name) {
        sequence_manager.flushGenerator(name);
    }

    /**
     * Requests of the sequence generator the next value from the sequence.
     * <p>
     * NOTE: This does NOT check that the user owning this connection has the
     * correct privs to perform this operation.
     */
    public long nextSequenceValue(TableName name) {
        if (isReadOnly()) {
            throw new RuntimeException(
                    "Sequence operation not permitted for read only transaction.");
        }
        // Check: if null sequence manager then sequence ops not allowed.
        if (sequence_manager == null) {
            throw new RuntimeException("Sequence operations are not permitted.");
        }

        SequenceManager seq = sequence_manager;
        long val = seq.nextValue(this, name);
        // No synchronized because a DatabaseConnection should be single threaded
        // only.
        sequence_value_cache.put(name, val);
        return val;
    }

    /**
     * Returns the sequence value for the given sequence generator that
     * was last returned by a call to 'nextSequenceValue'.  If a value was not
     * last returned by a call to 'nextSequenceValue' then a statement exception
     * is generated.
     * <p>
     * NOTE: This does NOT check that the user owning this connection has the
     * correct privs to perform this operation.
     */
    public long lastSequenceValue(TableName name) {
        // No synchronized because a DatabaseConnection should be single threaded
        // only.
        Long v = (Long) sequence_value_cache.get(name);
        if (v != null) {
            return v;
        } else {
            throw new StatementException(
                    "Current value for sequence generator " + name + " is not available.");
        }
    }

    /**
     * Sets the sequence value for the given sequence generator.  If the generator
     * does not exist or it is not possible to set the value for the generator
     * then an exception is generated.
     * <p>
     * NOTE: This does NOT check that the user owning this connection has the
     * correct privs to perform this operation.
     */
    public void setSequenceValue(TableName name, long value) {
        if (isReadOnly()) {
            throw new RuntimeException(
                    "Sequence operation not permitted for read only transaction.");
        }
        // Check: if null sequence manager then sequence ops not allowed.
        if (sequence_manager == null) {
            throw new RuntimeException("Sequence operations are not permitted.");
        }

        SequenceManager seq = sequence_manager;
        seq.setValue(this, name, value);

        sequence_value_cache.put(name, value);
    }

    /**
     * Returns the current unique id for the given table name.  Note that this
     * is NOT a view of the ID, it is the actual ID value at this time regardless
     * of transaction.
     */
    public long currentUniqueID(TableName table_name) {
        MasterTableDataSource master = findVisibleTable(table_name, false);
        if (master == null) {
            throw new StatementException(
                    "Table with name '" + table_name + "' could not be " +
                            "found to retrieve unique id.");
        }
        return master.currentUniqueID();
    }

    /**
     * Atomically returns a unique id that can be used as a seed for a set of
     * unique identifiers for a table.  Values returned by this method are
     * guarenteed unique within this table.  This is true even across
     * transactions.
     * <p>
     * NOTE: This change can not be rolled back.
     */
    public long nextUniqueID(TableName table_name) {
        if (isReadOnly()) {
            throw new RuntimeException(
                    "Sequence operation not permitted for read only transaction.");
        }

        MasterTableDataSource master = findVisibleTable(table_name, false);
        if (master == null) {
            throw new StatementException(
                    "Table with name '" + table_name + "' could not be " +
                            "found to retrieve unique id.");
        }
        return master.nextUniqueID();
    }

    /**
     * Sets the unique id for the given table name.  This must only be called
     * under very controlled situations, such as when altering a table or when
     * we need to fix sequence corruption.
     */
    public void setUniqueID(TableName table_name, long unique_id) {
        if (isReadOnly()) {
            throw new RuntimeException(
                    "Sequence operation not permitted for read only transaction.");
        }

        MasterTableDataSource master = findVisibleTable(table_name, false);
        if (master == null) {
            throw new StatementException(
                    "Table with name '" + table_name + "' could not be " +
                            "found to set unique id.");
        }
        master.setUniqueID(unique_id);
    }


}

