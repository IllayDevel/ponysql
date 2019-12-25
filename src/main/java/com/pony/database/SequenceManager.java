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

import com.pony.database.global.StringObject;
import com.pony.util.IntegerVector;
import com.pony.util.BigNumber;

import java.util.HashMap;

/**
 * An object that manages the creation and removal of sequence keys, and that
 * offers access to the sequence values (possibly cached).  When the sequence
 * table is changed, this opens an optimized transaction on the database and
 * manipulates the SequenceInfo table.
 *
 * @author Tobias Downer
 */

final class SequenceManager {

    /**
     * The TableDataConglomerate object.
     */
    private final TableDataConglomerate conglomerate;

    /**
     * A hashmap that maps from the TableName of the sequence key
     * to the object that manages this sequence (SequenceGenerator).
     * (TableName) -> (SequenceGenerator)
     */
    private final HashMap sequence_key_map;

    /**
     * A static TObject that represents numeric 1.
     */
    private static final TObject ONE_VAL = TObject.intVal(1);

    /**
     * A static TObject that represents boolean true.
     */
    private static final TObject TRUE_VAL = TObject.booleanVal(true);

    /**
     * Constructs the object.
     */
    SequenceManager(TableDataConglomerate conglomerate) {
        this.conglomerate = conglomerate;
        sequence_key_map = new HashMap();
    }


    /**
     * Returns a new Transaction object for manipulating and querying the system
     * state.
     */
    private Transaction getTransaction() {
        // Should this transaction be optimized for the access patterns we generate
        // here?
        return conglomerate.createTransaction();
    }

    /**
     * Returns a SequenceGenerator object representing the sequence generator
     * with the given name.
     */
    private SequenceGenerator getGenerator(TableName name) {
        // Is the generator already in the cache?
        SequenceGenerator generator =
                (SequenceGenerator) sequence_key_map.get(name);

        if (generator == null) {
            // This sequence generator is not in the cache so we need to query the
            // sequence table for this.
            Transaction sequence_access_transaction = getTransaction();
            try {
                MutableTableDataSource seqi =
                        sequence_access_transaction.getTable(TableDataConglomerate.SYS_SEQUENCE_INFO);
                SimpleTableQuery query = new SimpleTableQuery(seqi);

                StringObject schema_val = StringObject.fromString(name.getSchema());
                StringObject name_val = StringObject.fromString(name.getName());
                IntegerVector ivec = query.selectIndexesEqual(2, name_val, 1, schema_val);

                if (ivec.size() == 0) {
                    throw new StatementException("Sequence generator '" + name +
                            "' not found.");
                } else if (ivec.size() > 1) {
                    throw new RuntimeException(
                            "Assert failed: multiple sequence keys with same name.");
                }

                int row_i = ivec.intAt(0);
                TObject sid = seqi.getCellContents(0, row_i);
                TObject sschema = seqi.getCellContents(1, row_i);
                TObject sname = seqi.getCellContents(2, row_i);
                TObject stype = seqi.getCellContents(3, row_i);

                long id_val = sid.toBigNumber().longValue();

                query.dispose();

                // Is this a custom sequence generator?
                // (stype == 1) == true
                if (stype.operatorEquals(ONE_VAL).valuesEqual(TRUE_VAL)) {
                    // Native generator.
                    generator = new SequenceGenerator(id_val, name);
                } else {
                    // Query the sequence table.
                    MutableTableDataSource seq =
                            sequence_access_transaction.getTable(TableDataConglomerate.SYS_SEQUENCE);
                    query = new SimpleTableQuery(seq);

                    ivec = query.selectIndexesEqual(0, sid);

                    if (ivec.size() == 0) {
                        throw new RuntimeException(
                                "Sequence table does not contain sequence information.");
                    }
                    if (ivec.size() > 1) {
                        throw new RuntimeException(
                                "Sequence table contains multiple generators for id.");
                    }

                    row_i = ivec.intAt(0);
                    BigNumber last_value = seq.getCellContents(1, row_i).toBigNumber();
                    BigNumber increment = seq.getCellContents(2, row_i).toBigNumber();
                    BigNumber minvalue = seq.getCellContents(3, row_i).toBigNumber();
                    BigNumber maxvalue = seq.getCellContents(4, row_i).toBigNumber();
                    BigNumber start = seq.getCellContents(5, row_i).toBigNumber();
                    BigNumber cache = seq.getCellContents(6, row_i).toBigNumber();
                    Boolean cycle = seq.getCellContents(7, row_i).toBoolean();

                    query.dispose();

                    generator = new SequenceGenerator(id_val, name,
                            last_value.longValue(), increment.longValue(),
                            minvalue.longValue(), maxvalue.longValue(), start.longValue(),
                            cache.longValue(), cycle.booleanValue());

                    // Put the generator in the cache
                    sequence_key_map.put(name, generator);

                }

            } finally {
                // Make sure we always close and commit the transaction.
                try {
                    sequence_access_transaction.closeAndCommit();
                } catch (TransactionException e) {
                    conglomerate.Debug().writeException(e);
                    throw new RuntimeException("Transaction Error: " + e.getMessage());
                }
            }

        }

        // Return the generator
        return generator;
    }

    /**
     * Updates the state of the sequence key in the sequence tables in the
     * database.  The update occurs on an independant transaction.
     */
    private void updateGeneratorState(SequenceGenerator generator) {

        // We need to update the sequence key state.
        Transaction sequence_access_transaction = getTransaction();
        try {
            // The sequence table
            MutableTableDataSource seq = sequence_access_transaction.getTable(
                    TableDataConglomerate.SYS_SEQUENCE);
            // Find the row with the id for this generator.
            SimpleTableQuery query = new SimpleTableQuery(seq);
            IntegerVector ivec = query.selectIndexesEqual(0,
                    BigNumber.fromLong(generator.id));
            // Checks
            if (ivec.size() == 0) {
                throw new StatementException("Sequence '" + generator.name +
                        "' not found.");
            } else if (ivec.size() > 1) {
                throw new RuntimeException(
                        "Assert failed: multiple id for sequence.");
            }

            // Get the row position
            int row_i = ivec.intAt(0);

            // Create the RowData
            RowData row_data = new RowData(seq);

            // Set the content of the row data
            row_data.setColumnDataFromTObject(0, TObject.longVal(generator.id));
            row_data.setColumnDataFromTObject(1,
                    TObject.longVal(generator.last_value));
            row_data.setColumnDataFromTObject(2,
                    TObject.longVal(generator.increment_by));
            row_data.setColumnDataFromTObject(3,
                    TObject.longVal(generator.min_value));
            row_data.setColumnDataFromTObject(4,
                    TObject.longVal(generator.max_value));
            row_data.setColumnDataFromTObject(5,
                    TObject.longVal(generator.start));
            row_data.setColumnDataFromTObject(6,
                    TObject.longVal(generator.cache));
            row_data.setColumnDataFromTObject(7,
                    TObject.booleanVal(generator.cycle));

            // Update the row
            seq.updateRow(row_i, row_data);

            // Dispose the resources
            query.dispose();

        } finally {
            // Close and commit the transaction
            try {
                sequence_access_transaction.closeAndCommit();
            } catch (TransactionException e) {
                conglomerate.Debug().writeException(e);
                throw new RuntimeException("Transaction Error: " + e.getMessage());
            }
        }

    }

    /**
     * Flushes a sequence generator from the cache.  This should be used when a
     * sequence generator is altered or dropped from the database.
     */
    synchronized void flushGenerator(TableName name) {
        sequence_key_map.remove(name);
    }

    /**
     * Static convenience - adds an entry to the Sequence table for a native
     * table in the database.  This acts as a gateway between the native sequence
     * table function and the custom sequence generator.  Note that some of the
     * system tables and all of the VIEW tables will not have native sequence
     * generators and thus not have an entry in the sequence table.
     */
    static void addNativeTableGenerator(Transaction transaction,
                                        TableName table_name) {

        // If the SYS_SEQUENCE or SYS_SEQUENCE_INFO tables don't exist then
        // We can't add or remove native tables
        if (table_name.equals(TableDataConglomerate.SYS_SEQUENCE) ||
                table_name.equals(TableDataConglomerate.SYS_SEQUENCE_INFO) ||
                !transaction.tableExists(TableDataConglomerate.SYS_SEQUENCE) ||
                !transaction.tableExists(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
            return;
        }

        MutableTableDataSource table =
                transaction.getTable(TableDataConglomerate.SYS_SEQUENCE_INFO);
        long unique_id =
                transaction.nextUniqueID(TableDataConglomerate.SYS_SEQUENCE_INFO);

        RowData row_data = new RowData(table);
        row_data.setColumnDataFromObject(0, new Long(unique_id));
        row_data.setColumnDataFromObject(1, table_name.getSchema());
        row_data.setColumnDataFromObject(2, table_name.getName());
        row_data.setColumnDataFromObject(3, new Long(1));
        table.addRow(row_data);

    }

    /**
     * Static convenience - removes an entry in the Sequence table for a native
     * table in the database.
     */
    static void removeNativeTableGenerator(Transaction transaction,
                                           TableName table_name) {

        // If the SYS_SEQUENCE or SYS_SEQUENCE_INFO tables don't exist then
        // We can't add or remove native tables
        if (table_name.equals(TableDataConglomerate.SYS_SEQUENCE) ||
                table_name.equals(TableDataConglomerate.SYS_SEQUENCE_INFO) ||
                !transaction.tableExists(TableDataConglomerate.SYS_SEQUENCE) ||
                !transaction.tableExists(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
            return;
        }

        // The SEQUENCE and SEQUENCE_INFO table
        MutableTableDataSource seq =
                transaction.getTable(TableDataConglomerate.SYS_SEQUENCE);
        MutableTableDataSource seqi =
                transaction.getTable(TableDataConglomerate.SYS_SEQUENCE_INFO);

        SimpleTableQuery query = new SimpleTableQuery(seqi);
        IntegerVector ivec =
                query.selectIndexesEqual(2, TObject.stringVal(table_name.getName()),
                        1, TObject.stringVal(table_name.getSchema()));

        // Remove the corresponding entry in the SEQUENCE table
        for (int i = 0; i < ivec.size(); ++i) {
            int row_i = ivec.intAt(i);
            TObject sid = seqi.getCellContents(0, row_i);

            SimpleTableQuery query2 = new SimpleTableQuery(seq);
            IntegerVector ivec2 = query2.selectIndexesEqual(0, sid);
            for (int n = 0; n < ivec2.size(); ++n) {
                // Remove entry from the sequence table.
                seq.removeRow(ivec2.intAt(n));
            }

            // Remove entry from the sequence info table
            seqi.removeRow(row_i);

            query2.dispose();

        }

        query.dispose();

    }

    /**
     * Creates a new sequence generator with the given name and details.  Note
     * that this method does not check if the generator name clashes with an
     * existing database object.
     */
    static void createSequenceGenerator(Transaction transaction,
                                        TableName table_name, long start_value, long increment_by,
                                        long min_value, long max_value, long cache, boolean cycle) {

        // If the SYS_SEQUENCE or SYS_SEQUENCE_INFO tables don't exist then
        // we can't create the sequence generator
        if (!transaction.tableExists(TableDataConglomerate.SYS_SEQUENCE) ||
                !transaction.tableExists(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
            throw new RuntimeException("System sequence tables do not exist.");
        }

        // The SEQUENCE and SEQUENCE_INFO table
        MutableTableDataSource seq =
                transaction.getTable(TableDataConglomerate.SYS_SEQUENCE);
        MutableTableDataSource seqi =
                transaction.getTable(TableDataConglomerate.SYS_SEQUENCE_INFO);

        // All rows in 'sequence_info' that match this table name.
        SimpleTableQuery query = new SimpleTableQuery(seqi);
        IntegerVector ivec =
                query.selectIndexesEqual(2, TObject.stringVal(table_name.getName()),
                        1, TObject.stringVal(table_name.getSchema()));

        if (ivec.size() > 0) {
            throw new RuntimeException(
                    "Sequence generator with name '" + table_name + "' already exists.");
        }

        // Dispose the query object
        query.dispose();

        // Generate a unique id for the sequence info table
        long unique_id =
                transaction.nextUniqueID(TableDataConglomerate.SYS_SEQUENCE_INFO);

        // Insert the new row
        RowData row_data = new RowData(seqi);
        row_data.setColumnDataFromObject(0, new Long(unique_id));
        row_data.setColumnDataFromObject(1, table_name.getSchema());
        row_data.setColumnDataFromObject(2, table_name.getName());
        row_data.setColumnDataFromObject(3, new Long(2));
        seqi.addRow(row_data);

        // Insert into the SEQUENCE table.
        row_data = new RowData(seq);
        row_data.setColumnDataFromObject(0, new Long(unique_id));
        row_data.setColumnDataFromObject(1, new Long(start_value));
        row_data.setColumnDataFromObject(2, new Long(increment_by));
        row_data.setColumnDataFromObject(3, new Long(min_value));
        row_data.setColumnDataFromObject(4, new Long(max_value));
        row_data.setColumnDataFromObject(5, new Long(start_value));
        row_data.setColumnDataFromObject(6, new Long(cache));
        row_data.setColumnDataFromObject(7, new Boolean(cycle));
        seq.addRow(row_data);

    }

    static void dropSequenceGenerator(Transaction transaction,
                                      TableName table_name) {

        // If the SYS_SEQUENCE or SYS_SEQUENCE_INFO tables don't exist then
        // we can't create the sequence generator
        if (!transaction.tableExists(TableDataConglomerate.SYS_SEQUENCE) ||
                !transaction.tableExists(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
            throw new RuntimeException("System sequence tables do not exist.");
        }

        // Remove the table generator (delete SEQUENCE_INFO and SEQUENCE entry)
        removeNativeTableGenerator(transaction, table_name);

    }

    /**
     * Returns the next value from the sequence generator.  This will atomically
     * increment the sequence counter.
     */
    synchronized long nextValue(SimpleTransaction transaction,
                                TableName name) {

        SequenceGenerator generator = getGenerator(name);

        if (generator.type == 1) {
            // Native generator
            return transaction.nextUniqueID(
                    new TableName(name.getSchema(), name.getName()));
        } else {
            // Custom sequence generator
            long current_val = generator.current_val;

            // Increment the current value.
            generator.incrementCurrentValue();

            // Have we reached the current cached point?
            if (current_val == generator.last_value) {
                // Increment the generator
                for (int i = 0; i < generator.cache; ++i) {
                    generator.incrementLastValue();
                }

                // Update the state
                updateGeneratorState(generator);

            }

            return generator.current_val;
        }

    }

    /**
     * Returns the current value from the sequence generator.
     */
    synchronized long curValue(SimpleTransaction transaction,
                               TableName name) {

        SequenceGenerator generator = getGenerator(name);

        if (generator.type == 1) {
            // Native generator
            return transaction.nextUniqueID(
                    new TableName(name.getSchema(), name.getName()));
        } else {
            // Custom sequence generator
            return generator.current_val;
        }

    }

    /**
     * Sets the current value of the sequence generator.
     */
    synchronized void setValue(SimpleTransaction transaction,
                               TableName name,
                               long value) {

        SequenceGenerator generator = getGenerator(name);

        if (generator.type == 1) {
            // Native generator
            transaction.setUniqueID(
                    new TableName(name.getSchema(), name.getName()), value);
        } else {
            // Custom sequence generator
            generator.current_val = value;
            generator.last_value = value;

            // Update the state
            updateGeneratorState(generator);

        }

    }

    /**
     * Returns an InternalTableInfo object used to model the list of sequence
     * generators that are accessible within the given Transaction object.  This
     * is used to model all sequence generators that have been defined as tables.
     */
    static InternalTableInfo createInternalTableInfo(Transaction transaction) {
        return new SequenceInternalTableInfo(transaction);
    }


    // ---------- Inner classes ----------

    /**
     * An object that encapsulates information about the sequence key.
     */
    private static class SequenceGenerator {

        /**
         * The current value of this sequence generator.
         */
        long current_val;

        /**
         * The id value of this sequence key.
         */
        final long id;

        /**
         * The name of this sequence key.
         */
        final TableName name;

        /**
         * The type of this sequence key.
         */
        final int type;

        // The following values are only set if 'type' is not a native table
        // sequence.

        /**
         * The last value of this sequence key.  This value represents the value
         * of the sequence key in the persistence medium.
         */
        long last_value;

        /**
         * The number we increment the sequence key by.
         */
        long increment_by;

        /**
         * The minimum value of the sequence key.
         */
        long min_value;

        /**
         * The maximum value of the sequence key.
         */
        long max_value;

        /**
         * The start value of the sequence generator.
         */
        long start;

        /**
         * How many values we cache.
         */
        long cache;

        /**
         * True if the sequence key is cycled.
         */
        boolean cycle;


        SequenceGenerator(long id, TableName name) {
            type = 1;
            this.id = id;
            this.name = name;
        }

        SequenceGenerator(long id, TableName name, long last_value,
                          long increment_by, long min_value, long max_value,
                          long start, long cache, boolean cycle) {
            type = 2;
            this.id = id;
            this.name = name;
            this.last_value = last_value;
            this.current_val = last_value;
            this.increment_by = increment_by;
            this.min_value = min_value;
            this.max_value = max_value;
            this.start = start;
            this.cache = cache;
            this.cycle = cycle;
        }

        private long incrementValue(long val) {
            val += increment_by;
            if (val > max_value) {
                if (cycle) {
                    val = min_value;
                } else {
                    throw new StatementException("Sequence out of bounds.");
                }
            }
            if (val < min_value) {
                if (cycle) {
                    val = max_value;
                } else {
                    throw new StatementException("Sequence out of bounds.");
                }
            }
            return val;
        }

        void incrementCurrentValue() {
            current_val = incrementValue(current_val);
        }

        void incrementLastValue() {
            last_value = incrementValue(last_value);
        }

    }

    /**
     * An object that models the list of sequences as table objects in a
     * transaction.
     */
    private static class SequenceInternalTableInfo implements InternalTableInfo {

        final Transaction transaction;

        SequenceInternalTableInfo(Transaction transaction) {
            this.transaction = transaction;
        }

        private static DataTableDef createDataTableDef(String schema, String name) {
            // Create the DataTableDef that describes this entry
            DataTableDef def = new DataTableDef();
            def.setTableName(new TableName(schema, name));

            // Add column definitions
            def.addColumn(DataTableColumnDef.createNumericColumn("last_value"));
            def.addColumn(DataTableColumnDef.createNumericColumn("current_value"));
            def.addColumn(DataTableColumnDef.createNumericColumn("top_value"));
            def.addColumn(DataTableColumnDef.createNumericColumn("increment_by"));
            def.addColumn(DataTableColumnDef.createNumericColumn("min_value"));
            def.addColumn(DataTableColumnDef.createNumericColumn("max_value"));
            def.addColumn(DataTableColumnDef.createNumericColumn("start"));
            def.addColumn(DataTableColumnDef.createNumericColumn("cache"));
            def.addColumn(DataTableColumnDef.createBooleanColumn("cycle"));

            // Set to immutable
            def.setImmutable();

            // Return the data table def
            return def;
        }

        public int getTableCount() {
            final TableName SEQ = TableDataConglomerate.SYS_SEQUENCE;
            if (transaction.tableExists(SEQ)) {
                return transaction.getTable(SEQ).getRowCount();
            } else {
                return 0;
            }
        }

        public int findTableName(TableName name) {
            final TableName SEQ_INFO = TableDataConglomerate.SYS_SEQUENCE_INFO;
            if (transaction.realTableExists(SEQ_INFO)) {
                // Search the table.
                MutableTableDataSource table = transaction.getTable(SEQ_INFO);
                RowEnumeration row_e = table.rowEnumeration();
                int p = 0;
                while (row_e.hasMoreRows()) {
                    int row_index = row_e.nextRowIndex();
                    TObject seq_type = table.getCellContents(3, row_index);
                    if (!seq_type.operatorEquals(ONE_VAL).valuesEqual(TRUE_VAL)) {
                        TObject ob_name = table.getCellContents(2, row_index);
                        if (ob_name.getObject().toString().equals(name.getName())) {
                            TObject ob_schema = table.getCellContents(1, row_index);
                            if (ob_schema.getObject().toString().equals(name.getSchema())) {
                                // Match so return this
                                return p;
                            }
                        }
                        ++p;
                    }
                }
            }
            return -1;
        }

        public TableName getTableName(int i) {
            final TableName SEQ_INFO = TableDataConglomerate.SYS_SEQUENCE_INFO;
            if (transaction.realTableExists(SEQ_INFO)) {
                // Search the table.
                MutableTableDataSource table = transaction.getTable(SEQ_INFO);
                RowEnumeration row_e = table.rowEnumeration();
                int p = 0;
                while (row_e.hasMoreRows()) {
                    int row_index = row_e.nextRowIndex();
                    TObject seq_type = table.getCellContents(3, row_index);
                    if (!seq_type.operatorEquals(ONE_VAL).valuesEqual(TRUE_VAL)) {
                        if (i == p) {
                            TObject ob_schema = table.getCellContents(1, row_index);
                            TObject ob_name = table.getCellContents(2, row_index);
                            return new TableName(ob_schema.getObject().toString(),
                                    ob_name.getObject().toString());
                        }
                        ++p;
                    }
                }
            }
            throw new RuntimeException("Out of bounds.");
        }

        public boolean containsTableName(TableName name) {
            final TableName SEQ_INFO = TableDataConglomerate.SYS_SEQUENCE_INFO;
            // This set can not contain the table that is backing it, so we always
            // return false for that.  This check stops an annoying recursive
            // situation for table name resolution.
            if (name.equals(SEQ_INFO)) {
                return false;
            } else {
                return findTableName(name) != -1;
            }
        }

        public String getTableType(int i) {
            return "SEQUENCE";
        }

        public DataTableDef getDataTableDef(int i) {
            TableName table_name = getTableName(i);
            return createDataTableDef(table_name.getSchema(), table_name.getName());
        }

        public MutableTableDataSource createInternalTable(int index) {
            MutableTableDataSource table =
                    transaction.getTable(TableDataConglomerate.SYS_SEQUENCE_INFO);
            RowEnumeration row_e = table.rowEnumeration();
            int p = 0;
            int i;
            int row_i = -1;
            while (row_e.hasMoreRows() && row_i == -1) {
                i = row_e.nextRowIndex();

                // Is this is a type 1 sequence we ignore (native table sequence).
                TObject seq_type = table.getCellContents(3, i);
                if (!seq_type.operatorEquals(ONE_VAL).valuesEqual(TRUE_VAL)) {
                    if (p == index) {
                        row_i = i;
                    }
                    ++p;
                }

            }
            if (row_i != -1) {
                TObject seq_id = table.getCellContents(0, row_i);
                String schema = table.getCellContents(1, row_i).getObject().toString();
                String name = table.getCellContents(2, row_i).getObject().toString();

                TableName table_name = new TableName(schema, name);

                // Find this id in the 'sequence' table
                MutableTableDataSource seq_table =
                        transaction.getTable(TableDataConglomerate.SYS_SEQUENCE);
                SelectableScheme scheme = seq_table.getColumnScheme(0);
                IntegerVector ivec = scheme.selectEqual(seq_id);
                if (ivec.size() > 0) {
                    int seq_row_i = ivec.intAt(0);

                    // Generate the DataTableDef
                    final DataTableDef table_def = createDataTableDef(schema, name);

                    // Last value for this sequence generated by the transaction
                    TObject lv;
                    try {
                        lv = TObject.longVal(transaction.lastSequenceValue(table_name));
                    } catch (StatementException e) {
                        lv = TObject.longVal(-1);
                    }
                    final TObject last_value = lv;
                    // The current value of the sequence generator
                    SequenceManager manager =
                            transaction.getConglomerate().getSequenceManager();
                    final TObject current_value =
                            TObject.longVal(manager.curValue(transaction, table_name));

                    // Read the rest of the values from the SEQUENCE table.
                    final TObject top_value = seq_table.getCellContents(1, seq_row_i);
                    final TObject increment_by = seq_table.getCellContents(2, seq_row_i);
                    final TObject min_value = seq_table.getCellContents(3, seq_row_i);
                    final TObject max_value = seq_table.getCellContents(4, seq_row_i);
                    final TObject start = seq_table.getCellContents(5, seq_row_i);
                    final TObject cache = seq_table.getCellContents(6, seq_row_i);
                    final TObject cycle = seq_table.getCellContents(7, seq_row_i);

                    // Implementation of MutableTableDataSource that describes this
                    // sequence generator.
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
                                    return last_value;
                                case 1:
                                    return current_value;
                                case 2:
                                    return top_value;
                                case 3:
                                    return increment_by;
                                case 4:
                                    return min_value;
                                case 5:
                                    return max_value;
                                case 6:
                                    return start;
                                case 7:
                                    return cache;
                                case 8:
                                    return cycle;
                                default:
                                    throw new RuntimeException("Column out of bounds.");
                            }
                        }
                    };

                } else {
                    throw new RuntimeException("No SEQUENCE table entry for generator.");
                }

            } else {
                throw new RuntimeException("Index out of bounds.");
            }

        }

    }

}

