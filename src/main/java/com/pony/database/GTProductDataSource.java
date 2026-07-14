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

import java.util.ArrayList;

import com.pony.database.global.StandardMessages;

/**
 * An implementation of MutableTableDataSource that models information about
 * the software.
 * <p>
 * NOTE: This is not designed to be a long kept object.  It must not last
 *   beyond the lifetime of a transaction.
 *
 * @author Tobias Downer
 */

final class GTProductDataSource extends GTDataSource {

    /**
     * The list of info keys/values in this object.
     */
    private ArrayList key_value_pairs;

    /**
     * Constructor.
     */
    public GTProductDataSource(Transaction transaction) {
        super(transaction.getSystem());
        this.key_value_pairs = new ArrayList();
    }

    /**
     * Initialize the data source.
     */
    public GTProductDataSource init() {

        // Set up the product variables.
        key_value_pairs.add("name");
        key_value_pairs.add(StandardMessages.NAME);

        key_value_pairs.add("version");
        key_value_pairs.add(StandardMessages.VERSION);

        key_value_pairs.add("copyright");
        key_value_pairs.add(StandardMessages.COPYRIGHT);

        return this;
    }

    // ---------- Implemented from GTDataSource ----------

    public DataTableDef getDataTableDef() {
        return DEF_DATA_TABLE_DEF;
    }

    public int getRowCount() {
        return key_value_pairs.size() / 2;
    }

    public TObject getCellContents(final int column, final int row) {
        switch (column) {
            case 0:  // var
                return columnValue(column, key_value_pairs.get(row * 2));
            case 1:  // value
                return columnValue(column,
                        key_value_pairs.get((row * 2) + 1));
            default:
                throw new Error("Column out of bounds.");
        }
    }

    // ---------- Overwritten from GTDataSource ----------

    public void dispose() {
        super.dispose();
        key_value_pairs = null;
    }

    // ---------- Static ----------

    /**
     * The data table def that describes this table of data source.
     */
    static final DataTableDef DEF_DATA_TABLE_DEF;

    static {

        DataTableDef def = new DataTableDef();
        def.setTableName(
                new TableName(Database.SYSTEM_SCHEMA, "ProductInfo"));

        // Add column definitions
        def.addColumn(stringColumn("var"));
        def.addColumn(stringColumn("value"));

        // Set to immutable
        def.setImmutable();

        DEF_DATA_TABLE_DEF = def;

    }

}
