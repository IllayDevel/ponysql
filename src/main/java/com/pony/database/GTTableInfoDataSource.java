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

import java.util.Arrays;

/**
 * An implementation of MutableTableDataSource that presents information
 * about the tables in all schema.
 * <p>
 * NOTE: This is not designed to be a long kept object.  It must not last
 *   beyond the lifetime of a transaction.
 *
 * @author Tobias Downer
 */

final class GTTableInfoDataSource extends GTDataSource {

    /**
     * The transaction that is the view of this information.
     */
    private Transaction transaction;

    /**
     * The list of all TableName visible to the transaction.
     */
    private TableName[] table_list;

    /**
     * The list of all table types that are visible.
     */
    private String[] table_types;

    /**
     * The number of rows in this table.
     */
    private int row_count;

    /**
     * Constructor.
     */
    public GTTableInfoDataSource(Transaction transaction) {
        super(transaction.getSystem());
        this.transaction = transaction;
    }

    /**
     * Initialize the data source.
     */
    public GTTableInfoDataSource init() {
        // All the tables
        table_list = transaction.getTableList();
        Arrays.sort(table_list);
        table_types = new String[table_list.length];
        row_count = table_list.length;

        for (int i = 0; i < table_list.length; ++i) {
            String cur_type = transaction.getTableType(table_list[i]);
            // If the table is in the SYS_INFO schema, the type is defined as a
            // SYSTEM TABLE.
            if (cur_type.equals("TABLE") &&
                    table_list[i].getSchema().equals("SYS_INFO")) {
                cur_type = "SYSTEM TABLE";
            }
            table_types[i] = cur_type;
        }

        return this;
    }

    // ---------- Implemented from GTDataSource ----------

    public DataTableDef getDataTableDef() {
        return DEF_DATA_TABLE_DEF;
    }

    public int getRowCount() {
        return row_count;
    }

    public TObject getCellContents(final int column, final int row) {
        final TableName tname = table_list[row];
        switch (column) {
            case 0:  // schema
                return columnValue(column, tname.getSchema());
            case 1:  // name
                return columnValue(column, tname.getName());
            case 2:  // type
                return columnValue(column, table_types[row]);
            case 3:  // other
                // Table notes, etc.  (future enhancement)
                return columnValue(column, "");
            default:
                throw new Error("Column out of bounds.");
        }
    }

    // ---------- Overwritten from GTDataSource ----------

    public void dispose() {
        super.dispose();
        table_list = null;
        transaction = null;
    }

    // ---------- Static ----------

    /**
     * The data table def that describes this table of data source.
     */
    static final DataTableDef DEF_DATA_TABLE_DEF;

    static {

        DataTableDef def = new DataTableDef();
        def.setTableName(new TableName(Database.SYSTEM_SCHEMA, "TableInfo"));

        // Add column definitions
        def.addColumn(stringColumn("schema"));
        def.addColumn(stringColumn("name"));
        def.addColumn(stringColumn("type"));
        def.addColumn(stringColumn("other"));

        // Set to immutable
        def.setImmutable();

        DEF_DATA_TABLE_DEF = def;

    }

}
