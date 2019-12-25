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

import java.util.ArrayList;

/**
 * An implementation of MutableTableDataSource that presents the current
 * connection information.
 * <p>
 * NOTE: This is not designed to be a long kept object.  It must not last
 *   beyond the lifetime of a transaction.
 *
 * @author Tobias Downer
 */

final class GTConnectionInfoDataSource extends GTDataSource {

    /**
     * The DatabaseConnection object that this is table is modelling the
     * information within.
     */
    private DatabaseConnection database;

    /**
     * The list of info keys/values in this object.
     */
    private ArrayList key_value_pairs;

    /**
     * Constructor.
     */
    public GTConnectionInfoDataSource(DatabaseConnection connection) {
        super(connection.getSystem());
        this.database = connection;
        this.key_value_pairs = new ArrayList();
    }

    /**
     * Initialize the data source.
     */
    public GTConnectionInfoDataSource init() {

        // Set up the connection info variables.
        key_value_pairs.add("auto_commit");
        key_value_pairs.add(database.getAutoCommit() ? "true" : "false");

        key_value_pairs.add("isolation_level");
        key_value_pairs.add(database.getTransactionIsolationAsString());

        key_value_pairs.add("user");
        key_value_pairs.add(database.getUser().getUserName());

        key_value_pairs.add("time_connection");
        key_value_pairs.add(new java.sql.Timestamp(
                database.getUser().getTimeConnected()).toString());

        key_value_pairs.add("connection_string");
        key_value_pairs.add(database.getUser().getConnectionString());

        key_value_pairs.add("current_schema");
        key_value_pairs.add(database.getCurrentSchema());

        key_value_pairs.add("case_insensitive_identifiers");
        key_value_pairs.add(database.isInCaseInsensitiveMode() ? "true" : "false");

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
                return columnValue(column, (String) key_value_pairs.get(row * 2));
            case 1:  // value
                return columnValue(column,
                        (String) key_value_pairs.get((row * 2) + 1));
            default:
                throw new Error("Column out of bounds.");
        }
    }

    // ---------- Overwritten from GTDataSource ----------

    public void dispose() {
        super.dispose();
        key_value_pairs = null;
        database = null;
    }

    // ---------- Static ----------

    /**
     * The data table def that describes this table of data source.
     */
    static final DataTableDef DEF_DATA_TABLE_DEF;

    static {

        DataTableDef def = new DataTableDef();
        def.setTableName(
                new TableName(Database.SYSTEM_SCHEMA, "ConnectionInfo"));

        // Add column definitions
        def.addColumn(stringColumn("var"));
        def.addColumn(stringColumn("value"));

        // Set to immutable
        def.setImmutable();

        DEF_DATA_TABLE_DEF = def;

    }

}
