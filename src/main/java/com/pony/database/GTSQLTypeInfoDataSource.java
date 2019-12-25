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

import com.pony.util.BigNumber;
import com.pony.database.global.SQLTypes;

/**
 * A GTDataSource that models all SQL types that are available.
 * <p>
 * NOTE: This is not designed to be a long kept object.  It must not last
 *   beyond the lifetime of a transaction.
 *
 * @author Tobias Downer
 */

public class GTSQLTypeInfoDataSource extends GTDataSource {

    /**
     * The DatabaseConnection object.  Currently this is not used, but it may
     * be needed in the future if user-defined SQL types are supported.
     */
    private DatabaseConnection database;

    /**
     * The list of info keys/values in this object.
     */
    private ArrayList key_value_pairs;

    /**
     * Constant for type_nullable types.
     */
    private static final BigNumber TYPE_NULLABLE =
            BigNumber.fromInt(java.sql.DatabaseMetaData.typeNullable);

    /**
     * Constructor.
     */
    public GTSQLTypeInfoDataSource(DatabaseConnection connection) {
        super(connection.getSystem());
        this.database = connection;
        this.key_value_pairs = new ArrayList();
    }

    /**
     * Adds a type description.
     */
    private void addType(String name, int type, int precision,
                         String prefix, String suffix, String oops,
                         boolean searchable) {
        key_value_pairs.add(name);
        key_value_pairs.add(BigNumber.fromLong(type));
        key_value_pairs.add(BigNumber.fromLong(precision));
        key_value_pairs.add(prefix);
        key_value_pairs.add(suffix);
        key_value_pairs.add(searchable ? BigNumber.fromLong(3) :
                BigNumber.fromLong(0));
    }

    /**
     * Initialize the data source.
     */
    public GTSQLTypeInfoDataSource init() {

        addType("BIT", SQLTypes.BIT, 1, null, null, null, true);
        addType("BOOLEAN", SQLTypes.BIT, 1, null, null, null, true);
        addType("TINYINT", SQLTypes.TINYINT, 9, null, null, null, true);
        addType("SMALLINT", SQLTypes.SMALLINT, 9, null, null, null, true);
        addType("INTEGER", SQLTypes.INTEGER, 9, null, null, null, true);
        addType("BIGINT", SQLTypes.BIGINT, 9, null, null, null, true);
        addType("FLOAT", SQLTypes.FLOAT, 9, null, null, null, true);
        addType("REAL", SQLTypes.REAL, 9, null, null, null, true);
        addType("DOUBLE", SQLTypes.DOUBLE, 9, null, null, null, true);
        addType("NUMERIC", SQLTypes.NUMERIC, 9, null, null, null, true);
        addType("DECIMAL", SQLTypes.DECIMAL, 9, null, null, null, true);
        addType("CHAR", SQLTypes.CHAR, 9, "'", "'", null, true);
        addType("VARCHAR", SQLTypes.VARCHAR, 9, "'", "'", null, true);
        addType("LONGVARCHAR", SQLTypes.LONGVARCHAR, 9, "'", "'", null, true);
        addType("DATE", SQLTypes.DATE, 9, null, null, null, true);
        addType("TIME", SQLTypes.TIME, 9, null, null, null, true);
        addType("TIMESTAMP", SQLTypes.TIMESTAMP, 9, null, null, null, true);
        addType("BINARY", SQLTypes.BINARY, 9, null, null, null, false);
        addType("VARBINARY", SQLTypes.VARBINARY, 9, null, null, null, false);
        addType("LONGVARBINARY", SQLTypes.LONGVARBINARY,
                9, null, null, null, false);
        addType("JAVA_OBJECT", SQLTypes.JAVA_OBJECT, 9, null, null, null, false);

        return this;
    }

    // ---------- Implemented from GTDataSource ----------

    public DataTableDef getDataTableDef() {
        return DEF_DATA_TABLE_DEF;
    }

    public int getRowCount() {
        return key_value_pairs.size() / 6;
    }

    public TObject getCellContents(final int column, final int row) {
        int i = (row * 6);
        switch (column) {
            case 0:  // type_name
                return columnValue(column, (String) key_value_pairs.get(i));
            case 1:  // data_type
                return columnValue(column, (BigNumber) key_value_pairs.get(i + 1));
            case 2:  // precision
                return columnValue(column, (BigNumber) key_value_pairs.get(i + 2));
            case 3:  // literal_prefix
                return columnValue(column, (String) key_value_pairs.get(i + 3));
            case 4:  // literal_suffix
                return columnValue(column, (String) key_value_pairs.get(i + 4));
            case 5:  // create_params
                return columnValue(column, null);
            case 6:  // nullable
                return columnValue(column, TYPE_NULLABLE);
            case 7:  // case_sensitive
                return columnValue(column, Boolean.TRUE);
            case 8:  // searchable
                return columnValue(column, (BigNumber) key_value_pairs.get(i + 5));
            case 9:  // unsigned_attribute
                return columnValue(column, Boolean.FALSE);
            case 10:  // fixed_prec_scale
                return columnValue(column, Boolean.FALSE);
            case 11:  // auto_increment
                return columnValue(column, Boolean.FALSE);
            case 12:  // local_type_name
                return columnValue(column, null);
            case 13:  // minimum_scale
                return columnValue(column, BigNumber.fromLong(0));
            case 14:  // maximum_scale
                return columnValue(column, BigNumber.fromLong(10000000));
            case 15:  // sql_data_type
                return columnValue(column, null);
            case 16:  // sql_datetype_sub
                return columnValue(column, null);
            case 17:  // num_prec_radix
                return columnValue(column, BigNumber.fromLong(10));
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
                new TableName(Database.SYSTEM_SCHEMA, "SQLTypeInfo"));

        // Add column definitions
        def.addColumn(stringColumn("TYPE_NAME"));
        def.addColumn(numericColumn("DATA_TYPE"));
        def.addColumn(numericColumn("PRECISION"));
        def.addColumn(stringColumn("LITERAL_PREFIX"));
        def.addColumn(stringColumn("LITERAL_SUFFIX"));
        def.addColumn(stringColumn("CREATE_PARAMS"));
        def.addColumn(numericColumn("NULLABLE"));
        def.addColumn(booleanColumn("CASE_SENSITIVE"));
        def.addColumn(numericColumn("SEARCHABLE"));
        def.addColumn(booleanColumn("UNSIGNED_ATTRIBUTE"));
        def.addColumn(booleanColumn("FIXED_PREC_SCALE"));
        def.addColumn(booleanColumn("AUTO_INCREMENT"));
        def.addColumn(stringColumn("LOCAL_TYPE_NAME"));
        def.addColumn(numericColumn("MINIMUM_SCALE"));
        def.addColumn(numericColumn("MAXIMUM_SCALE"));
        def.addColumn(stringColumn("SQL_DATA_TYPE"));
        def.addColumn(stringColumn("SQL_DATETIME_SUB"));
        def.addColumn(numericColumn("NUM_PREC_RADIX"));

        // Set to immutable
        def.setImmutable();

        DEF_DATA_TABLE_DEF = def;

    }

}
