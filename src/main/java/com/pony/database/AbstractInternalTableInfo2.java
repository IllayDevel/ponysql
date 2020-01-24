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

/**
 * An implementation of InternalTableInfo that provides a number of methods to
 * aid in the productions of the InternalTableInfo interface for a transaction
 * specific model of a set of tables that is based on a single system table.
 * This would be used to model table views for triggers, views, procedures and
 * sequences all of which are table sets tied to a single table respectively,
 * and the number of items in the table represent the number of tables to model.
 * <p>
 * This abstraction assumes that the name of the schema/table are in columns 0
 * and 1 of the backed system table.
 *
 * @author Tobias Downer
 */

abstract class AbstractInternalTableInfo2 implements InternalTableInfo {

    /**
     * The transaction we are connected to.
     */
    protected final Transaction transaction;

    /**
     * The table in the transaction that contains the list of items we are
     * modelling.
     */
    protected final TableName table_name;

    /**
     * Constructor.
     */
    public AbstractInternalTableInfo2(Transaction transaction,
                                      TableName table_name) {
        this.transaction = transaction;
        this.table_name = table_name;
    }

    public int getTableCount() {
        if (transaction.tableExists(table_name)) {
            return transaction.getTable(table_name).getRowCount();
        } else {
            return 0;
        }
    }

    public int findTableName(TableName name) {
        if (transaction.realTableExists(table_name)) {
            // Search the table.  We assume that the schema and name of the object
            // are in columns 0 and 1 respectively.
            MutableTableDataSource table = transaction.getTable(table_name);
            RowEnumeration row_e = table.rowEnumeration();
            int p = 0;
            while (row_e.hasMoreRows()) {
                int row_index = row_e.nextRowIndex();
                TObject ob_name = table.getCellContents(1, row_index);
                if (ob_name.getObject().toString().equals(name.getName())) {
                    TObject ob_schema = table.getCellContents(0, row_index);
                    if (ob_schema.getObject().toString().equals(name.getSchema())) {
                        // Match so return this
                        return p;
                    }
                }
                ++p;
            }
        }
        return -1;
    }

    public TableName getTableName(int i) {
        if (transaction.realTableExists(table_name)) {
            // Search the table.  We assume that the schema and name of the object
            // are in columns 0 and 1 respectively.
            MutableTableDataSource table = transaction.getTable(table_name);
            RowEnumeration row_e = table.rowEnumeration();
            int p = 0;
            while (row_e.hasMoreRows()) {
                int row_index = row_e.nextRowIndex();
                if (i == p) {
                    TObject ob_schema = table.getCellContents(0, row_index);
                    TObject ob_name = table.getCellContents(1, row_index);
                    return new TableName(ob_schema.getObject().toString(),
                            ob_name.getObject().toString());
                }
                ++p;
            }
        }
        throw new RuntimeException("Out of bounds.");
    }

    public boolean containsTableName(TableName name) {
        // This set can not contain the table that is backing it, so we always
        // return false for that.  This check stops an annoying recursive
        // situation for table name resolution.
        if (name.equals(table_name)) {
            return false;
        } else {
            return findTableName(name) != -1;
        }
    }

    public abstract DataTableDef getDataTableDef(int i);

    public abstract String getTableType(int i);

    public abstract MutableTableDataSource createInternalTable(int index);

}

