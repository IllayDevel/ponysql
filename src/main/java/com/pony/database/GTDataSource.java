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

import com.pony.database.global.SQLTypes;

/**
 * A base class for a dynamically generated data source.  While this inherits
 * MutableTableDataSource (so we can make a DataTable out of it) a GTDataSource
 * derived class may not be mutable.  For example, an implementation of this
 * class may produce a list of a columns in all tables.  You would typically
 * not want a user to change this information unless they run a DML command.
 *
 * @author Tobias Downer
 */

abstract class GTDataSource implements MutableTableDataSource {

    /**
     * The TransactionSystem object for this table.
     */
    private final TransactionSystem system;


    /**
     * Constructor.
     */
    public GTDataSource(TransactionSystem system) {
        this.system = system;
    }

    /**
     * Returns a TObject that represents a value for the given column in this
     * table.  The Object must be of a compatible class to store in the type
     * of the column defined.
     */
    protected TObject columnValue(int column, Object ob) {
        TType type = getDataTableDef().columnAt(column).getTType();
        return new TObject(type, ob);
    }

    // ---------- Implemented from TableDataSource ----------

    public TransactionSystem getSystem() {
        return system;
    }

    public abstract DataTableDef getDataTableDef();

    public abstract int getRowCount();

    public RowEnumeration rowEnumeration() {
        return new SimpleRowEnumeration(getRowCount());
    }

    public SelectableScheme getColumnScheme(int column) {
        return new BlindSearch(this, column);
    }

    public abstract TObject getCellContents(final int column, final int row);

    // ---------- Implemented from MutableTableDataSource ----------

    public int addRow(RowData row_data) {
        throw new RuntimeException("Functionality not available.");
    }

    public void removeRow(int row_index) {
        throw new RuntimeException("Functionality not available.");
    }

    public int updateRow(int row_index, RowData row_data) {
        throw new RuntimeException("Functionality not available.");
    }

    public MasterTableJournal getJournal() {
        throw new RuntimeException("Functionality not available.");
    }

    public void flushIndexChanges() {
        throw new RuntimeException("Functionality not available.");
    }

    public void constraintIntegrityCheck() {
        throw new RuntimeException("Functionality not available.");
    }

    public void dispose() {
    }

    public void addRootLock() {
        // No need to lock roots
    }

    public void removeRootLock() {
        // No need to lock roots
    }

    // ---------- Static ----------

    /**
     * Convenience methods for constructing a DataTableDef for the dynamically
     * generated table.
     */
    protected static DataTableColumnDef stringColumn(String name) {
        DataTableColumnDef column = new DataTableColumnDef();
        column.setName(name);
        column.setNotNull(true);
        column.setSQLType(SQLTypes.VARCHAR);
        column.setSize(Integer.MAX_VALUE);
        column.setScale(-1);
        column.setIndexScheme("BlindSearch");
        column.initTTypeInfo();
        return column;
    }

    protected static DataTableColumnDef booleanColumn(String name) {
        DataTableColumnDef column = new DataTableColumnDef();
        column.setName(name);
        column.setNotNull(true);
        column.setSQLType(SQLTypes.BIT);
        column.setSize(-1);
        column.setScale(-1);
        column.setIndexScheme("BlindSearch");
        column.initTTypeInfo();
        return column;
    }

    protected static DataTableColumnDef numericColumn(String name) {
        DataTableColumnDef column = new DataTableColumnDef();
        column.setName(name);
        column.setNotNull(true);
        column.setSQLType(SQLTypes.NUMERIC);
        column.setSize(-1);
        column.setScale(-1);
        column.setIndexScheme("BlindSearch");
        column.initTTypeInfo();
        return column;
    }

    protected static DataTableColumnDef dateColumn(String name) {
        DataTableColumnDef column = new DataTableColumnDef();
        column.setName(name);
        column.setNotNull(true);
        column.setSQLType(SQLTypes.TIMESTAMP);
        column.setSize(-1);
        column.setScale(-1);
        column.setIndexScheme("BlindSearch");
        column.initTTypeInfo();
        return column;
    }

}
