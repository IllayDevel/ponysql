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

import com.pony.util.Stats;

/**
 * An implementation of MutableTableDataSource that presents database
 * statistical information.
 * <p>
 * NOTE: This is not designed to be a long kept object.  It must not last
 *   beyond the lifetime of a transaction.
 *
 * @author Tobias Downer
 */

final class GTStatisticsDataSource extends GTDataSource {

    /**
     * Contains all the statistics information for this session.
     */
    private String[] statistics_info;

    /**
     * The system database stats.
     */
    private Stats stats;

    /**
     * Constructor.
     */
    public GTStatisticsDataSource(DatabaseConnection connection) {
        super(connection.getSystem());
        stats = connection.getDatabase().stats();
    }

    /**
     * Initialize the data source.
     */
    public GTStatisticsDataSource init() {

        synchronized (stats) {
            stats.set((int) (Runtime.getRuntime().freeMemory() / 1024),
                    "Runtime.memory.freeKB");
            stats.set((int) (Runtime.getRuntime().totalMemory() / 1024),
                    "Runtime.memory.totalKB");

            String[] key_set = stats.keyList();
            int glob_length = key_set.length * 2;
            statistics_info = new String[glob_length];

            for (int i = 0; i < glob_length; i += 2) {
                String key_name = key_set[i / 2];
                statistics_info[i] = key_name;
                statistics_info[i + 1] = stats.statString(key_name);
            }

        }
        return this;
    }

    // ---------- Implemented from GTDataSource ----------

    public DataTableDef getDataTableDef() {
        return DEF_DATA_TABLE_DEF;
    }

    public int getRowCount() {
        return statistics_info.length / 2;
    }

    public TObject getCellContents(final int column, final int row) {
        switch (column) {
            case 0:  // stat_name
                return columnValue(column, statistics_info[row * 2]);
            case 1:  // value
                return columnValue(column, statistics_info[(row * 2) + 1]);
            default:
                throw new Error("Column out of bounds.");
        }
    }

    // ---------- Overwritten from GTDataSource ----------

    public void dispose() {
        super.dispose();
        statistics_info = null;
        stats = null;
    }

    // ---------- Static ----------

    /**
     * The data table def that describes this table of data source.
     */
    static final DataTableDef DEF_DATA_TABLE_DEF;

    static {

        DataTableDef def = new DataTableDef();
        def.setTableName(
                new TableName(Database.SYSTEM_SCHEMA, "DatabaseStatistics"));

        // Add column definitions
        def.addColumn(stringColumn("stat_name"));
        def.addColumn(stringColumn("value"));

        // Set to immutable
        def.setImmutable();

        DEF_DATA_TABLE_DEF = def;

    }

}
