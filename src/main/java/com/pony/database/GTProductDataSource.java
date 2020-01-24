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
