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
import com.pony.util.BlockIntegerList;

import java.util.ArrayList;

/**
 * The list of all primitive operations to the database that a transaction
 * performed.  It includes the list of all rows added or removed to all tables,
 * and the tables created and dropped and any table that had constraint
 * modifications.
 * <p>
 * This journal is updated inside a Transaction.  When the transaction is
 * completed, this journal is used both to determine if the transaction
 * can be committed, and also to update the changes to the data that a
 * transaction has made.
 * <p>
 * THREADING: The journal update commands are synchronized because they need
 *  to be atomic operations and can be accessed by multiple threads.
 *
 * @author Tobias Downer
 */

final class TransactionJournal {

    /**
     * Journal commands.
     */
    static final byte TABLE_ADD = 1;  // Add a row to a table.
    // (params: table_id, row_index)
    static final byte TABLE_REMOVE = 2;  // Remove a row from a table.
    // (params: table_id, row_index)
    static final byte TABLE_CREATE = 3;  // Create a new table.
    // (params: table_id)
    static final byte TABLE_DROP = 4;  // Drop a table.
    // (params: table_id)
    static final byte TABLE_CONSTRAINT_ALTER = 5; // Alter constraints of a table.
    // (params: table_id)

    /**
     * The number of entries in this journal.
     */
    private int journal_entries;

    /**
     * The list of table's that have been touched by this transaction.  A table
     * is touched if the 'getTable' method in the transaction is used to
     * get the table.  This means even if a table is just read from, the
     * journal will record that the table was touched.
     * <p>
     * This object records the 'table_id' of the touched tables in a sorted
     * list.
     */
    private final IntegerVector touched_tables;

    /**
     * A byte[] array that represents the set of commands a transaction
     * performed on a table.
     */
    private byte[] command_journal;

    /**
     * An IntegerVector that is filled with parameters from the command journal.
     * For example, a 'TABLE_ADD' journal log will have as parameters the
     * table id the row was added to, and the row_index that was added.
     */
    private final IntegerVector command_parameters;

    private boolean has_created_tables;
    private boolean has_dropped_tables;
    private boolean has_constraint_alterations;

    /**
     * Constructs a blank journal.
     */
    TransactionJournal() {
        journal_entries = 0;
        command_journal = new byte[16];
        command_parameters = new IntegerVector(32);
        touched_tables = new IntegerVector(8);

        /**
         * Optimization, these flags are set to true when various types of journal
         * entries are made to the transaction journal.
         */
        boolean has_added_table_rows = false;
        boolean has_removed_table_rows = false;
        has_created_tables = false;
        has_dropped_tables = false;
        has_constraint_alterations = false;
    }

    /**
     * Adds a command to the journal.
     */
    private void addCommand(byte command) {
        if (journal_entries >= command_journal.length) {
            // Resize command array.
            int grow_size = Math.min(4000, journal_entries);
            byte[] new_command_journal = new byte[journal_entries + grow_size];
            System.arraycopy(command_journal, 0, new_command_journal, 0,
                    journal_entries);
            command_journal = new_command_journal;
        }

        command_journal[journal_entries] = command;
        ++journal_entries;
    }

    /**
     * Adds a parameter to the journal command parameters.
     */
    private void addParameter(int param) {
        command_parameters.addInt(param);
    }

    /**
     * Logs in this journal that the transaction touched the given table id.
     */
    synchronized void entryAddTouchedTable(int table_id) {
        int pos = touched_tables.sortedIndexOf(table_id);
        // If table_id already in the touched table list.
        if (pos < touched_tables.size() &&
                touched_tables.intAt(pos) == table_id) {
            return;
        }
        // If position to insert >= size of the touched tables set then add to
        // the end of the set.
        if (pos >= touched_tables.size()) {
            touched_tables.addInt(table_id);
        } else {
            // Otherwise, insert into sorted order.
            touched_tables.insertIntAt(table_id, pos);
        }
    }

    /**
     * Makes a journal entry that a table entry has been added to the table with
     * the given id.
     */
    synchronized void entryAddTableRow(int table_id, int row_index) {
//    has_added_table_rows = true;
        addCommand(TABLE_ADD);
        addParameter(table_id);
        addParameter(row_index);
    }

    /**
     * Makes a journal entry that a table entry has been removed from the table
     * with the given id.
     */
    synchronized void entryRemoveTableRow(int table_id, int row_index) {
//    has_removed_table_rows = true;
        addCommand(TABLE_REMOVE);
        addParameter(table_id);
        addParameter(row_index);
    }

    /**
     * Makes a journal entry that a table with the given 'table_id' has been
     * created by this transaction.
     */
    synchronized void entryTableCreate(int table_id) {
        has_created_tables = true;
        addCommand(TABLE_CREATE);
        addParameter(table_id);
    }

    /**
     * Makes a journal entry that a table with the given 'table_id' has been
     * dropped by this transaction.
     */
    synchronized void entryTableDrop(int table_id) {
        has_dropped_tables = true;
        addCommand(TABLE_DROP);
        addParameter(table_id);
    }

    /**
     * Makes a journal entry that a table with the given 'table_id' has been
     * altered by this transaction.
     */
    synchronized void entryTableConstraintAlter(int table_id) {
        has_constraint_alterations = true;
        addCommand(TABLE_CONSTRAINT_ALTER);
        addParameter(table_id);
    }


    /**
     * Generates an array of MasterTableJournal objects that specify the
     * changes that occur to each table affected by this transaction.  Each array
     * element represents a change to an individual table in the conglomerate
     * that changed as a result of this transaction.
     * <p>
     * This is used when a transaction successfully commits and we need to log
     * the transaction changes with the master table.
     * <p>
     * If no changes occurred to a table, then no entry is returned here.
     */
    MasterTableJournal[] makeMasterTableJournals() {
        ArrayList table_journals = new ArrayList();
        int param_index = 0;

        MasterTableJournal master_journal = null;

        for (int i = 0; i < journal_entries; ++i) {
            byte c = command_journal[i];
            if (c == TABLE_ADD || c == TABLE_REMOVE) {
                int table_id = command_parameters.intAt(param_index);
                int row_index = command_parameters.intAt(param_index + 1);
                param_index += 2;

                // Do we already have this table journal?
                if (master_journal == null ||
                        master_journal.getTableID() != table_id) {
                    // Try to find the journal in the list.
                    int size = table_journals.size();
                    master_journal = null;
                    for (int n = 0; n < size && master_journal == null; ++n) {
                        MasterTableJournal test_journal =
                                (MasterTableJournal) table_journals.get(n);
                        if (test_journal.getTableID() == table_id) {
                            master_journal = test_journal;
                        }
                    }

                    // Not found so add to list.
                    if (master_journal == null) {
                        master_journal = new MasterTableJournal(table_id);
                        table_journals.add(master_journal);
                    }

                }

                // Add this change to the table journal.
                master_journal.addEntry(c, row_index);

            } else if (c == TABLE_CREATE ||
                    c == TABLE_DROP ||
                    c == TABLE_CONSTRAINT_ALTER) {
                param_index += 1;
            } else {
                throw new Error("Unknown journal command.");
            }
        }

        // Return the array.
        return (MasterTableJournal[]) table_journals.toArray(
                new MasterTableJournal[table_journals.size()]);

    }

    /**
     * Returns the list of tables id's that were dropped by this journal.
     */
    IntegerVector getTablesDropped() {
        IntegerVector dropped_tables = new IntegerVector();
        // Optimization, quickly return empty set if we know there are no tables.
        if (!has_dropped_tables) {
            return dropped_tables;
        }

        int param_index = 0;
        for (int i = 0; i < journal_entries; ++i) {
            byte c = command_journal[i];
            if (c == TABLE_ADD || c == TABLE_REMOVE) {
                param_index += 2;
            } else if (c == TABLE_CREATE || c == TABLE_CONSTRAINT_ALTER) {
                param_index += 1;
            } else if (c == TABLE_DROP) {
                dropped_tables.addInt(command_parameters.intAt(param_index));
                param_index += 1;
            } else {
                throw new Error("Unknown journal command.");
            }
        }

        return dropped_tables;
    }

    /**
     * Returns the list of tables id's that were created by this journal.
     */
    IntegerVector getTablesCreated() {
        IntegerVector created_tables = new IntegerVector();
        // Optimization, quickly return empty set if we know there are no tables.
        if (!has_created_tables) {
            return created_tables;
        }

        int param_index = 0;
        for (int i = 0; i < journal_entries; ++i) {
            byte c = command_journal[i];
            if (c == TABLE_ADD || c == TABLE_REMOVE) {
                param_index += 2;
            } else if (c == TABLE_DROP || c == TABLE_CONSTRAINT_ALTER) {
                param_index += 1;
            } else if (c == TABLE_CREATE) {
                created_tables.addInt(command_parameters.intAt(param_index));
                param_index += 1;
            } else {
                throw new Error("Unknown journal command.");
            }
        }

        return created_tables;
    }

    /**
     * Returns the list of tables id's that were constraint altered by this
     * journal.
     */
    IntegerVector getTablesConstraintAltered() {
        IntegerVector caltered_tables = new IntegerVector();
        // Optimization, quickly return empty set if we know there are no tables.
        if (!has_constraint_alterations) {
            return caltered_tables;
        }

        int param_index = 0;
        for (int i = 0; i < journal_entries; ++i) {
            byte c = command_journal[i];
            if (c == TABLE_ADD || c == TABLE_REMOVE) {
                param_index += 2;
            } else if (c == TABLE_DROP || c == TABLE_CREATE) {
                param_index += 1;
            } else if (c == TABLE_CONSTRAINT_ALTER) {
                caltered_tables.addInt(command_parameters.intAt(param_index));
                param_index += 1;
            } else {
                throw new Error("Unknown journal command.");
            }
        }

        return caltered_tables;
    }


}
