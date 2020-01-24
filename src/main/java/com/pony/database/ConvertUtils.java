/*
 * Pony SQL Database ( http://i-devel.ru )
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

import com.pony.debug.DebugLogger;
import com.pony.util.ByteArrayUtil;
import com.pony.util.IntegerListInterface;

/**
 * Various static database convertion tools for converting for upgrading
 * parts of the database.
 *
 * @author Tobias Downer
 */

class ConvertUtils {


    /**
     * Upgrades an .ijf index file to an .iid IndexStore.  With version 0.92
     * of the database we introduced a specialized scalable IndexStore for
     * storing all indexing information.
     * <p>
     * Returns an list of MasterTableJournal that contains any journal entries
     * that are pending to be made to the table.
     */
    static ArrayList<Object> convertIndexFiles1(File original_ijf,
                                        IndexStore new_store, DataTableDef table_def,
                                        DebugLogger logger) throws IOException {

        int column_count = table_def.columnCount();

        // Open the old ijf file
        FixedSizeDataStore ijf =
                new FixedSizeDataStore(original_ijf, -1, false, logger);
        ijf.open(false);

        int block_size = 1024;
        if (table_def.getTableName().getSchema().equals("SYS_INFO")) {
            block_size = 128;
        }

        // Create and initialize the new index store
        new_store.create(block_size);
        new_store.init();
        new_store.addIndexLists(column_count + 1, (byte) 1);
        new_store.flush();
        IndexSet index_set = new_store.getSnapshotIndexSet();

        // Load the index header.
        int header_size = 8 + 4 + 4 + (column_count * 4);
        byte[] index_header_data = new byte[header_size];
        byte[] reserved_header = new byte[64];
        ijf.readReservedBuffer(reserved_header, 0, 64);
        // Get the current 'unique_id' value.
        long unique_id = ByteArrayUtil.getLong(reserved_header, 8);
        int cur_header_sector = ByteArrayUtil.getInt(reserved_header, 0);
        ijf.readAcross(cur_header_sector,
                index_header_data, 0, index_header_data.length);
        // 'index_header_data' will now contain the header format.

        // ---

        // Convert the master index first,
        // Where is the information in the header file?
        int mast_index_sector = ByteArrayUtil.getInt(index_header_data, 8);
        InputStream sin = ijf.getSectorInputStream(mast_index_sector);
        DataInputStream din = new DataInputStream(sin);

        int ver = din.readInt();   // The version.
        if (ver != 1) {
            throw new IOException("Unrecognised master index list version.");
        }

        // The master index is always at 0.
        IntegerListInterface master_index = index_set.getIndex(0);
        int entries_count = din.readInt();
        int previous = -1;
        for (int i = 0; i < entries_count; ++i) {
            int entry = din.readInt();
            if (entry == previous) {
                throw new IOException("Master index format corrupt - double entry.");
            } else if (entry < previous) {
                throw new IOException("Master index format corrupt - not sorted.");
            }
            master_index.add(entry);
        }

        // Close the stream
        din.close();

        // ---

        // Any journal modifications
        // Where is the information in the header file?
        int journal_sector = ByteArrayUtil.getInt(index_header_data, 12);
        sin = ijf.getSectorInputStream(journal_sector);
        din = new DataInputStream(sin);

        ver = din.readInt();   // The version.
        if (ver != 1) {
            throw new Error("Unrecognised journals list version.");
        }

        ArrayList<Object> transaction_mod_list = new ArrayList<>();
        int num_journals = din.readInt();
        for (int i = 0; i < num_journals; ++i) {
            MasterTableJournal journal = new MasterTableJournal();
            journal.readFrom(din);
            transaction_mod_list.add(journal);
        }

        // Close the stream
        din.close();

        // ---

        // Convert the indices for each column
        // This is the new made up list of indices
        IntegerListInterface[] column_indices =
                new IntegerListInterface[column_count];

        // For each column
        for (int column = 0; column < column_count; ++column) {
            // First check this is an indexable type.
            if (table_def.columnAt(column).isIndexableType()) {
                // Where is the information in the header file?
                int scheme_sector = ByteArrayUtil.getInt(index_header_data,
                        16 + (column * 4));

                sin = ijf.getSectorInputStream(scheme_sector);
                din = new DataInputStream(sin);

                // Read the type of scheme for this column (1=Insert, 2=Blind).
                byte t = (byte) din.read();

                if (t == 1) {
                    // The index list for the given column
                    IntegerListInterface col_index = index_set.getIndex(column + 1);
                    column_indices[column] = col_index;

                    // Read from the input and output to the list.
                    int vec_size = din.readInt();
                    for (int i = 0; i < vec_size; ++i) {
                        int row = din.readInt();
                        col_index.add(row);
                    }

                } else {
                    // Ignore otherwise
                }

                // Close the stream
                din.close();

            }  // If column is indexable

        }  // for each column

        // ---

        // Commit the new index store changes
        new_store.commitIndexSet(index_set);
        // Dispose of the set
        index_set.dispose();
        // Set the unique id
        new_store.setUniqueID(unique_id);
        // Flush the changes and synchronize with the file system.
        new_store.flush();
        new_store.hardSynch();

        // Close and delete the old ijf file
        ijf.close();
        ijf.delete();

        // Return the list of MasterTableJournal
        return transaction_mod_list;

    }


}
