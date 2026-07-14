/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
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
