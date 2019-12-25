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

package com.pony.database.jdbc;

import com.pony.database.global.ColumnDescription;
import com.pony.database.global.SQLTypes;

import java.sql.*;
import java.math.BigDecimal;

/**
 * An implementation of JDBC's ResultSetmetaData.
 *
 * @author Tobias Downer
 */

public class MResultSetMetaData implements ResultSetMetaData {

    /**
     * The parent MResultSet object.
     */
    private final MResultSet result_set;

    /**
     * Constructs the ResultSetMetaData over the given result set.
     */
    MResultSetMetaData(MResultSet result_set) {
        this.result_set = result_set;
    }

    /**
     * Returns the object class that a given sql_type will produce by the
     * 'getObject' call in ResultSet.
     */
    private static Class jdbcSQLClass(int sql_type) {
        switch (sql_type) {
            case (SQLTypes.BIT):
                return Boolean.class;
            case (SQLTypes.TINYINT):
                return Byte.class;
            case (SQLTypes.SMALLINT):
                return Short.class;
            case (SQLTypes.INTEGER):
                return Integer.class;
            case (SQLTypes.BIGINT):
                return Long.class;
            case (SQLTypes.FLOAT):
                return Double.class;
            case (SQLTypes.REAL):
                return Float.class;
            case (SQLTypes.DOUBLE):
                return Double.class;
            case (SQLTypes.NUMERIC):
                return BigDecimal.class;
            case (SQLTypes.DECIMAL):
                return BigDecimal.class;
            case (SQLTypes.CHAR):
                return String.class;
            case (SQLTypes.VARCHAR):
                return String.class;
            case (SQLTypes.LONGVARCHAR):
                return String.class;
            case (SQLTypes.DATE):
                return java.sql.Date.class;
            case (SQLTypes.TIME):
                return java.sql.Time.class;
            case (SQLTypes.TIMESTAMP):
                return java.sql.Timestamp.class;
            case (SQLTypes.BINARY):
                return byte[].class;
            case (SQLTypes.VARBINARY):
                return byte[].class;
            case (SQLTypes.LONGVARBINARY):
                return byte[].class;
            case (SQLTypes.NULL):
                return Object.class;
            case (SQLTypes.OTHER):
                return Object.class;
            case (SQLTypes.JAVA_OBJECT):
                return Object.class;
            case (SQLTypes.DISTINCT):
                // (Not supported)
                return Object.class;
            case (SQLTypes.STRUCT):
                // (Not supported)
                return Object.class;
            case (SQLTypes.ARRAY):
                // (Not supported)
                return Object.class;
//#IFDEF(JDBC2.0)
            case (SQLTypes.BLOB):
                return java.sql.Blob.class;
            case (SQLTypes.CLOB):
                return java.sql.Clob.class;
            case (SQLTypes.REF):
                // (Not supported)
                return Object.class;
//#ENDIF
            default:
                return Object.class;
        }
    }

    // ---------- Implemented from ResultSetMetaData ----------

    public int getColumnCount() throws SQLException {
        return result_set.columnCount();
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        // There are no hard-coded auto increment columns but you can make one
        // with the UNIQUEKEY function.
        return false;
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    public boolean isSearchable(int column) throws SQLException {
        return result_set.getColumn(column - 1).isQuantifiable();
    }

    public boolean isCurrency(int column) throws SQLException {
        // Currency not supported by the driver or the database.
        return false;
    }

    public int isNullable(int column) throws SQLException {
        if (result_set.getColumn(column - 1).isNotNull()) {
            return columnNoNulls;
        } else {
            return columnNullable;
        }
    }

    public boolean isSigned(int column) throws SQLException {
        // There are no unsigned numbers....
        // All other types aren't signed (strings, dates, etc)
        return result_set.getColumn(column - 1).isNumericType();
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        // How can we implement this when strings and numbers
        // can be any length?
        return 64;
    }

    public String getColumnLabel(int column) throws SQLException {
        // ISSUE: Should this process be cached?  Could be a problem if this
        //   method is used in an inner loop.  (A string object is created)
        String encoded_name = result_set.getColumn(column - 1).getName();
        if (encoded_name.startsWith("@a")) {
            // Strip any control characters and return
            return encoded_name.substring(2);
        } else if (encoded_name.startsWith("@f")) {
            // Return only the column name, not the schema.table part
            int p = encoded_name.lastIndexOf(".");
            if (p > -1) {
                return encoded_name.substring(p + 1);
            } else {
                return encoded_name.substring(2);
            }
        }
        // No encoding (must be an older version of the database engine).
        return encoded_name;
    }

    public String getColumnName(int column) throws SQLException {
        // If the JDBC driver is set to succinct column names (the default) then
        // return what 'getColumnLabel' tells us.
        if (!result_set.verboseColumnNames()) {
            return getColumnLabel(column);
        } else {
            // ISSUE: Should this process be cached?  Could be a problem if this
            //   method is used in an inner loop.  (A string object is created)
            String encoded_name = result_set.getColumn(column - 1).getName();
            if (encoded_name.startsWith("@")) {
                // Strip any control characters and return
                return encoded_name.substring(2);
            }
            // No encoding (must be an older version of the database engine).
            return encoded_name;
        }
    }

    public String getSchemaName(int column) throws SQLException {
        ColumnDescription col = result_set.getColumn(column - 1);
        String name = col.getName();

        // Do we have a column code.  If not default to 'f'
        char col_code = 'f';
        int name_start = 0;
        if (name.startsWith("@")) {
            col_code = name.charAt(1);
            name_start = 2;
        }

        if (col_code == 'a') {
            // This is an alias so there is no table name
            return "";
        } else if (col_code == 'f') {
            // Assume it is [schema_name].[table_name].[column_name]
            int delim = name.lastIndexOf(".");
            if (delim == -1) {
                return "";
            } else {
                delim = name.lastIndexOf(".", delim - 1);
                if (delim == -1) {
                    return "";
                } else {
                    int end_point = delim;
                    delim = name.lastIndexOf(".", delim - 1);
                    if (delim == -1) {
                        return name.substring(name_start, end_point);
                    } else {
                        return name.substring(delim + 1, end_point);
                    }
                }
            }
        } else {
            throw new SQLException("Unknown column code: '" + col_code + "'");
        }
    }

    public int getPrecision(int column) throws SQLException {
        // HACK: Precision is not a property we define for columns yet....
        // For *CHAR columns, we make this return the max size of the string
        int size = result_set.getColumn(column - 1).getSize();
        if (size == -1) {
            size = 32;
        }
        return size;
    }

    public int getScale(int column) throws SQLException {
        int scale = result_set.getColumn(column - 1).getScale();
        if (scale == -1) {
            scale = 0;
        }
        return scale;
    }

    public String getTableName(int column) throws SQLException {
        ColumnDescription col = result_set.getColumn(column - 1);
        String name = col.getName();

        // Do we have a column code.  If not default to 'f'
        char col_code = 'f';
        int name_start = 0;
        if (name.startsWith("@")) {
            col_code = name.charAt(1);
            name_start = 2;
        }

        if (col_code == 'a') {
            // This is an alias so there is no table name
            return "";
        } else if (col_code == 'f') {
            // Assume it is [schema_name].[table_name].[column_name]
            int delim = name.lastIndexOf(".");
            if (delim == -1) {
                return "";
            } else {
                int end_point = delim;
                delim = name.lastIndexOf(".", end_point - 1);
                if (delim == -1) {
                    return name.substring(name_start, end_point);
                } else {
                    return name.substring(delim + 1, end_point);
                }
            }
        } else {
            throw new SQLException("Unknown column code: '" + col_code + "'");
        }
    }

    public String getCatalogName(int column) throws SQLException {
        // No support for catalogs
        return "";
    }

    public int getColumnType(int column) throws SQLException {
        return result_set.getColumn(column - 1).getSQLType();
    }

    public String getColumnTypeName(int column) throws SQLException {
        return result_set.getColumn(column - 1).getSQLTypeName();
    }

    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

//#IFDEF(JDBC2.0)

    //--------------------------JDBC 2.0-----------------------------------

    public String getColumnClassName(int column) throws SQLException {
        // PENDING: This should return the instance class name set as the
        //   constraint for a JAVA_OBJECT column.
        return jdbcSQLClass(result_set.getColumn(column - 1).getSQLType()).getName();
//    return result_set.getColumn(column - 1).classType().getName();
    }

//#ENDIF

//#IFDEF(JDBC4.0)

    // -------------------------- JDK 1.6 -----------------------------------

    public Object unwrap(Class iface) throws SQLException {
        throw MSQLException.unsupported();
    }

    public boolean isWrapperFor(Class iface) throws SQLException {
        throw MSQLException.unsupported16();
    }

//#ENDIF
}
