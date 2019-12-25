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

package com.pony.database.jdbc;

import com.pony.util.Cache;
import com.pony.database.global.ObjectTransfer;

import java.util.Vector;
import java.io.*;
import java.sql.SQLException;

/**
 * A Cache that stores rows retrieved from the server in result set's.  This
 * provides various mechanisms for determining the best rows to pick out that
 * haven't been cached, etc.
 *
 * @author Tobias Downer
 */

final class RowCache {

    /**
     * The actual cache that stores the rows.
     */
    private final Cache row_cache;

    /**
     * Constructs the cache.
     *
     * @param cache_size the number of elements in the row cache.
     * @param max_size the maximum size of the combined total of all items in
     *   the cache.
     */
    RowCache(int cache_size, int max_size) {
        row_cache = new Cache(cache_size, cache_size, 20);
    }

    /**
     * Requests a block of parts.  If the block can be completely retrieved from
     * the cache then it is done so.  Otherwise, it forwards the request for the
     * rows onto the connection object.
     */
    synchronized Vector getResultPart(Vector result_block,
                                      MConnection connection, int result_id, int row_index, int row_count,
                                      int col_count, int total_row_count) throws IOException, SQLException {

        // What was requested....
        int orig_row_index = row_index;
        int orig_row_count = row_count;

        Vector rows = new Vector();

        // The top row that isn't found in the cache.
        boolean found_notcached = false;
        // Look for the top row in the block that hasn't been cached
        for (int r = 0; r < row_count && !found_notcached; ++r) {
            int da_row = row_index + r;
            // Is the row in the cache?
            RowRef row_ref = new RowRef(result_id, da_row);
            // Not in cache so mark this as top row not in cache...
            CachedRow row = (CachedRow) row_cache.get(row_ref);
            if (row == null) {
                row_index = da_row;
                if (row_index + row_count > total_row_count) {
                    row_count = total_row_count - row_index;
                }
                found_notcached = true;
            } else {
                rows.addElement(row);
            }
        }

        Vector rows2 = new Vector();
        if (found_notcached) {

            // Now work up from the bottom and find row that isn't in cache....
            found_notcached = false;
            // Look for the bottom row in the block that hasn't been cached
            for (int r = row_count - 1; r >= 0 && !found_notcached; --r) {
                int da_row = row_index + r;
                // Is the row in the cache?
                RowRef row_ref = new RowRef(result_id, da_row);
                // Not in cache so mark this as top row not in cache...
                CachedRow row = (CachedRow) row_cache.get(row_ref);
                if (row == null) {
                    if (row_index == orig_row_index) {
                        row_index = row_index - (row_count - (r + 1));
                        if (row_index < 0) {
                            row_count = row_count + row_index;
                            row_index = 0;
                        }
                    } else {
                        row_count = r + 1;
                    }
                    found_notcached = true;
                } else {
                    rows2.insertElementAt(row, 0);
                }
            }

        }

        // Some of it not in the cache...
        if (found_notcached) {
//      System.out.println("REQUESTING: " + row_index + " - " + row_count);
            // Request a part of a result from the server (blocks)
            ResultPart block = connection.requestResultPart(result_id,
                    row_index, row_count);

            int block_index = 0;
            for (int r = 0; r < row_count; ++r) {
                Object[] arr = new Object[col_count];
                int da_row = (row_index + r);
                int col_size = 0;
                for (int c = 0; c < col_count; ++c) {
                    Object ob = block.elementAt(block_index);
                    ++block_index;
                    arr[c] = ob;
                    col_size += ObjectTransfer.size(ob);
                }

                CachedRow cached_row = new CachedRow();
                cached_row.row = da_row;
                cached_row.row_data = arr;

                // Don't cache if it's over a certain size,
                if (col_size <= 3200) {
                    row_cache.put(new RowRef(result_id, da_row), cached_row);
                }
                rows.addElement(cached_row);
            }

        }

        // At this point, the cached rows should be completely in the cache so
        // retrieve it from the cache.
        result_block.removeAllElements();
        int low = orig_row_index;
        int high = orig_row_index + orig_row_count;
        for (int r = 0; r < rows.size(); ++r) {
            CachedRow row = (CachedRow) rows.elementAt(r);
            // Put into the result block
            if (row.row >= low && row.row < high) {
                for (int c = 0; c < col_count; ++c) {
                    result_block.addElement(row.row_data[c]);
                }
            }
        }
        for (int r = 0; r < rows2.size(); ++r) {
            CachedRow row = (CachedRow) rows2.elementAt(r);
            // Put into the result block
            if (row.row >= low && row.row < high) {
                for (int c = 0; c < col_count; ++c) {
                    result_block.addElement(row.row_data[c]);
                }
            }
        }

        // And return the result (phew!)
        return result_block;
    }

    /**
     * Flushes the complete contents of the cache.
     */
    synchronized void clear() {
        row_cache.removeAll();
    }


    // ---------- Inner classes ----------

    /**
     * Used for the hash key in the cache.
     */
    private final static class RowRef {
        final int table_id;
        final int row;

        RowRef(int table_id, int row) {
            this.table_id = table_id;
            this.row = row;
        }

        public int hashCode() {
            return (int) table_id + (row * 35331);
        }

        public boolean equals(Object ob) {
            RowRef dest = (RowRef) ob;
            return (row == dest.row && table_id == dest.table_id);
        }
    }

    /**
     * A cached row.
     */
    private final static class CachedRow {
        int row;
        Object[] row_data;
    }

}
