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

import java.io.*;

/**
 * A journal of changes that occured to a table in a data conglomerate during
 * a transaction.
 *
 * @author Tobias Downer
 */

final class MasterTableJournal {

    /**
     * Journal commands.
     */
    final static byte TABLE_ADD = 1;         // Add a row to a table.
    // (params: table_id, row_index)
    final static byte TABLE_REMOVE = 2;         // Remove a row from a table.
    // (params: table_id, row_index)
    final static byte TABLE_UPDATE_ADD = 5;  // Add a row from an update.
    final static byte TABLE_UPDATE_REMOVE = 6;  // Remove a row from an update.

    /**
     * The commit id given to this change when it is committed.  This is only
     * set when the journal is a committed change to the database.
     */
    private long commit_id;


    /**
     * The master table id.
     */
    private int table_id;

    /**
     * The number of entries in this journal.
     */
    private int journal_entries;

    /**
     * A byte[] array that represents the set of commands a transaction
     * performed on this table.
     */
    private byte[] command_journal;

    /**
     * An IntegerVector that is filled with parameters from the command journal.
     * For example, a 'TABLE_ADD' journal log will have as parameters the
     * row_index that was added to this table.
     */
    private final IntegerVector command_parameters;

    /**
     * Constructs the master table journal.
     */
    MasterTableJournal(int table_id) {
        this.table_id = table_id;
        command_journal = new byte[16];
        command_parameters = new IntegerVector(32);
    }

    MasterTableJournal() {
        this(-1);
    }

    /**
     * Sets the 'commit_id'.  This is only set when this change becomes a
     * committed change to the database.
     */
    void setCommitID(long commit_id) {
        this.commit_id = commit_id;
    }

    /**
     * Returns true if the given command is an addition command.
     */
    static boolean isAddCommand(byte command) {
        return ((command & 0x03) == TABLE_ADD);
    }

    /**
     * Returns true if the given command is a removal command.
     */
    static boolean isRemoveCommand(byte command) {
        return ((command & 0x03) == TABLE_REMOVE);
    }

    /**
     * Adds a command to the journal.
     */
    private void addCommand(byte command) {
        if (journal_entries >= command_journal.length) {
            // Resize command array.
            int grow_size = Math.min(4000, journal_entries);
            grow_size = Math.max(4, grow_size);
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
     * Removes the top n entries from the journal.
     */
    private void removeTopEntries(int n) {
        journal_entries = journal_entries - n;
        command_parameters.crop(0, command_parameters.size() - n);
    }

    /**
     * Adds a new command to this journal.
     */
    void addEntry(byte command, int row_index) {
        addCommand(command);
        addParameter(row_index);
    }

    // ---------- Getters ----------
    // These methods assume the journal has been setup and no more entries
    // will be made.

    /**
     * Returns the commit_id that has been set for this journal.
     */
    long getCommitID() {
        return commit_id;
    }

    /**
     * Returns the table id of the master table this journal is for.
     */
    int getTableID() {
        return table_id;
    }

    /**
     * Returns the total number of journal entries.
     */
    int entries() {
        return journal_entries;
    }

    /**
     * Returns the command of the nth entry in the journal.
     */
    byte getCommand(int n) {
        return command_journal[n];
    }

    /**
     * Returns the row index of the nth entry in the journal.
     */
    int getRowIndex(int n) {
        return command_parameters.intAt(n);
    }

    /**
     * Returns a normalized list of all rows that were added in this journal,
     * but not including those rows also removed.  For example, if rows
     * 1, 2, and 3 were added and 2 was removed, this will return a list of
     * 1 and 3.
     */
    int[] normalizedAddedRows() {
        IntegerVector list = new IntegerVector();
        int size = entries();
        for (int i = 0; i < size; ++i) {
            byte tc = getCommand(i);
            if (tc == TABLE_ADD || tc == TABLE_UPDATE_ADD) {
                int row_index = getRowIndex(i);
                // If row added, add to list
                list.addInt(row_index);
            } else if (tc == TABLE_REMOVE || tc == TABLE_UPDATE_REMOVE) {
                // If row removed, if the row is already in the list
                // it's removed from the list, otherwise we leave as is.
                int row_index = getRowIndex(i);
                int found_at = list.indexOf(row_index);
                if (found_at != -1) {
                    list.removeIntAt(found_at);
                }
            } else {
                throw new Error("Unknown command in journal.");
            }
        }

        return list.toIntArray();
    }

    /**
     * Returns a normalized list of all rows that were removed from this
     * journal.
     */
    int[] normalizedRemovedRows() {
        IntegerVector list = new IntegerVector();
        int size = entries();
        for (int i = 0; i < size; ++i) {
            byte tc = getCommand(i);
            if (tc == TABLE_REMOVE || tc == TABLE_UPDATE_REMOVE) {
                // If removed add to the list.
                int row_index = getRowIndex(i);
                list.addInt(row_index);
            }
        }
        return list.toIntArray();
    }

    /**
     * Returns three lists - a list of all rows that were inserted, a list of all
     * rows that were deleted, and a list of all updates.  All the lists are
     * ordered by the order of the command.  The update list contains two
     * entries per 'update', the row that was removed and the row that was
     * added with the updated info.
     * <p>
     * This method is useful for collecting all modification information on the
     * table.
     */
    IntegerVector[] allChangeInformation() {
        IntegerVector[] lists = new IntegerVector[3];
        for (int i = 0; i < 3; ++i) {
            lists[i] = new IntegerVector();
        }
        int size = entries();
        for (int i = 0; i < size; ++i) {
            byte tc = getCommand(i);
            int row_index = getRowIndex(i);
            if (tc == TABLE_ADD) {
                lists[0].addInt(row_index);
            } else if (tc == TABLE_REMOVE) {
                lists[1].addInt(row_index);
            } else if (tc == TABLE_UPDATE_ADD || tc == TABLE_UPDATE_REMOVE) {
                lists[2].addInt(row_index);
            } else {
                throw new RuntimeException("Don't understand journal command.");
            }
        }
        return lists;
    }

    /**
     * Rolls back the last n entries of this journal.  This method takes into
     * account the transient nature of rows (all added rows in the journal are
     * exclusively referenced by this journal).  The algorithm works as follows;
     * any rows added are deleted, and rows deleted (that weren't added) are
     * removed from the journal.
     */
    void rollbackEntries(int n) {
        if (n > journal_entries) {
            throw new RuntimeException(
                    "Trying to roll back more journal entries than are in the journal.");
        }

        IntegerVector to_add = new IntegerVector();

        // Find all entries and added new rows to the table
        int size = entries();
        for (int i = size - n; i < size; ++i) {
            byte tc = getCommand(i);
            if (tc == TABLE_ADD || tc == TABLE_UPDATE_ADD) {
                to_add.addInt(getRowIndex(i));
            }
        }

        // Delete the top entries
        removeTopEntries(n);
        // Mark all added entries to deleted.
        for (int i = 0; i < to_add.size(); ++i) {
            addEntry(TABLE_ADD, to_add.intAt(i));
            addEntry(TABLE_REMOVE, to_add.intAt(i));
        }

    }


    // ---------- Testing methods ----------

    /**
     * Throws a transaction clash exception if it detects a clash between
     * journal entries.  It assumes that this journal is the journal that is
     * attempting to be compatible with the given journal.  A journal clashes
     * when they both contain a row that is deleted.
     */
    void testCommitClash(DataTableDef table_def, MasterTableJournal journal)
            throws TransactionException {
        // Very nasty search here...
//    int cost = entries() * journal.entries();
//    System.out.print(" CLASH COST = " + cost + " ");

        for (int i = 0; i < entries(); ++i) {
            byte tc = getCommand(i);
            if (isRemoveCommand(tc)) {   // command - row remove
                int row_index = getRowIndex(i);
//        System.out.println("* " + row_index);
                for (int n = 0; n < journal.entries(); ++n) {
//          System.out.print(" " + journal.getRowIndex(n));
                    if (isRemoveCommand(journal.getCommand(n)) &&
                            journal.getRowIndex(n) == row_index) {
                        throw new TransactionException(
                                TransactionException.ROW_REMOVE_CLASH,
                                "Concurrent Serializable Transaction Conflict(1): " +
                                        "Current row remove clash ( row: " + row_index + ", table: " +
                                        table_def.getTableName() + " )");
                    }
                }
//        System.out.println();
            }
        }
    }


    // ---------- Stream serialization methods ----------

    /**
     * Reads the journal entries from the given DataInputStream to this object.
     * <p>
     * This method is only around because we might need it to convert a
     * 0.91 era database that stored index data as journals in the file system.
     */
    void readFrom(DataInputStream din) throws IOException {
        commit_id = din.readInt();
        table_id = din.readInt();

        journal_entries = din.readInt();
        command_journal = new byte[journal_entries];
        din.readFully(command_journal, 0, journal_entries);
        int size = din.readInt();
        for (int i = 0; i < size; ++i) {
            command_parameters.addInt(din.readInt());
        }
    }

    /**
     * Debugging.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[MasterTableJournal] [");
        buf.append(commit_id);
        buf.append("] (");
        for (int i = 0; i < entries(); ++i) {
            byte c = getCommand(i);
            int row_index = getRowIndex(i);
            buf.append("(");
            buf.append(c);
            buf.append(")");
            buf.append(row_index);
            buf.append(" ");
        }
        buf.append(")");
        return new String(buf);
    }

}
