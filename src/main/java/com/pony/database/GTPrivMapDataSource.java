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

import com.pony.util.BigNumber;

/**
 * A GTDataSource that maps a Privs 11-bit set to strings that represent the
 * priv in human understandable string.  Each 11-bit priv set contains 12
 * entries for each bit that was set.
 * <p>
 * This table provides a convenient way to join the system grant table and
 * 'expand' the privs that are allowed though it.
 *
 * @author Tobias Downer
 */

public class GTPrivMapDataSource extends GTDataSource {

    /**
     * Number of bits.
     */
    private static final int BIT_COUNT = Privileges.BIT_COUNT;


    /**
     * Constructor.
     */
    public GTPrivMapDataSource(DatabaseConnection connection) {
        super(connection.getSystem());
    }

    // ---------- Implemented from GTDataSource ----------

    public DataTableDef getDataTableDef() {
        return DEF_DATA_TABLE_DEF;
    }

    public int getRowCount() {
        return (1 << BIT_COUNT) * BIT_COUNT;
    }

    public TObject getCellContents(final int column, final int row) {
        int c1 = row / BIT_COUNT;
        if (column == 0) {
            return columnValue(column, BigNumber.fromInt(c1));
        } else {
            int priv_bit = (1 << (row % BIT_COUNT));
            String priv_string = null;
            if ((c1 & priv_bit) != 0) {
                priv_string = Privileges.formatPriv(priv_bit);
            }
            return columnValue(column, priv_string);
        }
    }

    // ---------- Overwritten from GTDataSource ----------

    public SelectableScheme getColumnScheme(int column) {
        if (column == 0) {
            return new PrivMapSearch(this, column);
        } else {
            return new BlindSearch(this, column);
        }
    }

    // ---------- Static ----------

    /**
     * The data table def that describes this table of data source.
     */
    static final DataTableDef DEF_DATA_TABLE_DEF;

    static {

        DataTableDef def = new DataTableDef();
        def.setTableName(
                new TableName(Database.SYSTEM_SCHEMA, "PrivMap"));

        // Add column definitions
        def.addColumn(numericColumn("priv_bit"));
        def.addColumn(stringColumn("description"));

        // Set to immutable
        def.setImmutable();

        DEF_DATA_TABLE_DEF = def;

    }

    // ---------- Inner classes ----------

    /**
     * A SelectableScheme that makes searching on the 'priv_bit' column a lot
     * less painless!
     */
    private static final class PrivMapSearch extends CollatedBaseSearch {

        PrivMapSearch(TableDataSource table, int column) {
            super(table, column);
        }

        public SelectableScheme copy(TableDataSource table, boolean immutable) {
            // Return a fresh object.  This implementation has no state so we can
            // ignore the 'immutable' flag.
            return new BlindSearch(table, getColumn());
        }

        protected int searchFirst(TObject val) {
            if (val.isNull()) {
                return -1;
            }

            int num = ((BigNumber) val.getObject()).intValue();

            if (num < 0) {
                return -1;
            } else if (num > (1 << BIT_COUNT)) {
                return -(((1 << BIT_COUNT) * BIT_COUNT) + 1);
            }

            return (num * BIT_COUNT);
        }

        protected int searchLast(TObject val) {
            int p = searchFirst(val);
            if (p >= 0) {
                return p + (BIT_COUNT - 1);
            } else {
                return p;
            }
        }

    }

}

