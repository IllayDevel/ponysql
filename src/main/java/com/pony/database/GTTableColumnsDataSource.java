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

import com.pony.util.BigNumber;

/**
 * An implementation of MutableTableDataSource that presents information
 * about the columns of all tables in all schema.
 * <p>
 * NOTE: This is not designed to be a long kept object.  It must not last
 *   beyond the lifetime of a transaction.
 *
 * @author Tobias Downer
 */

final class GTTableColumnsDataSource extends GTDataSource {

    /**
     * The transaction that is the view of this information.
     */
    private Transaction transaction;

    /**
     * The list of all DataTableDef visible to the transaction.
     */
    private DataTableDef[] visible_tables;

    /**
     * The number of rows in this table.
     */
    private int row_count;

    /**
     * Constructor.
     */
    public GTTableColumnsDataSource(Transaction transaction) {
        super(transaction.getSystem());
        this.transaction = transaction;
    }

    /**
     * Initialize the data source.
     */
    public GTTableColumnsDataSource init() {
        // All the tables
        TableName[] list = transaction.getTableList();
        visible_tables = new DataTableDef[list.length];
        row_count = 0;
        for (int i = 0; i < list.length; ++i) {
            DataTableDef def = transaction.getDataTableDef(list[i]);
            row_count += def.columnCount();
            visible_tables[i] = def;
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

        final int sz = visible_tables.length;
        int rs = 0;
        for (final DataTableDef def : visible_tables) {
            final int b = rs;
            rs += def.columnCount();
            if (row >= b && row < rs) {
                // This is the column that was requested,
                int seq_no = row - b;
                DataTableColumnDef col_def = def.columnAt(seq_no);
                switch (column) {
                    case 0:  // schema
                        return columnValue(column, def.getSchema());
                    case 1:  // table
                        return columnValue(column, def.getName());
                    case 2:  // column
                        return columnValue(column, col_def.getName());
                    case 3:  // sql_type
                        return columnValue(column,
                                BigNumber.fromLong(col_def.getSQLType()));
                    case 4:  // type_desc
                        return columnValue(column, col_def.getSQLTypeString());
                    case 5:  // size
                        return columnValue(column, BigNumber.fromLong(col_def.getSize()));
                    case 6:  // scale
                        return columnValue(column, BigNumber.fromLong(col_def.getScale()));
                    case 7:  // not_null
                        return columnValue(column, col_def.isNotNull());
                    case 8:  // default
                        return columnValue(column,
                                col_def.getDefaultExpressionString());
                    case 9:  // index_str
                        return columnValue(column, col_def.getIndexScheme());
                    case 10:  // seq_no
                        return columnValue(column, BigNumber.fromLong(seq_no));
                    default:
                        throw new Error("Column out of bounds.");
                }
            }

        }  // for each visible table

        throw new Error("Row out of bounds.");
    }

    // ---------- Overwritten ----------

    public void dispose() {
        super.dispose();
        visible_tables = null;
        transaction = null;
    }

    // ---------- Static ----------

    /**
     * The data table def that describes this table of data source.
     */
    static final DataTableDef DEF_DATA_TABLE_DEF;

    static {

        DataTableDef def = new DataTableDef();
        def.setTableName(
                new TableName(Database.SYSTEM_SCHEMA, "TableColumns"));

        // Add column definitions
        def.addColumn(stringColumn("schema"));
        def.addColumn(stringColumn("table"));
        def.addColumn(stringColumn("column"));
        def.addColumn(numericColumn("sql_type"));
        def.addColumn(stringColumn("type_desc"));
        def.addColumn(numericColumn("size"));
        def.addColumn(numericColumn("scale"));
        def.addColumn(booleanColumn("not_null"));
        def.addColumn(stringColumn("default"));
        def.addColumn(stringColumn("index_str"));
        def.addColumn(numericColumn("seq_no"));

        // Set to immutable
        def.setImmutable();

        DEF_DATA_TABLE_DEF = def;

    }

}
