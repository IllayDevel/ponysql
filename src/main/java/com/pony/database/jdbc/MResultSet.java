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

import java.io.*;
import java.sql.*;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Vector;

import com.pony.database.global.ColumnDescription;
import com.pony.database.global.ByteLongObject;
import com.pony.database.global.StreamableObject;
import com.pony.database.global.ObjectTranslator;
import com.pony.database.global.SQLTypes;
import com.pony.database.global.StringObject;
import com.pony.util.BigNumber;

/**
 * Implementation of a ResultSet.
 * <p>
 * Multi-threaded issue:  This class is not designed to be multi-thread
 *  safe.  A ResultSet should not be accessed by concurrent threads.
 *
 * @author Tobias Downer
 */

public final class MResultSet implements ResultSet {

    /**
     * The default fetch size.
     */
    private static final int DEFAULT_FETCH_SIZE = 32;

    /**
     * The maximum fetch size.
     */
    private static final int MAXIMUM_FETCH_SIZE = 512;

    /**
     * The current unique id key.
     */
    private static int unique_id_key = 1;

    /**
     * BigNumber for 0.
     */
    private static final BigNumber BD_ZERO = BigNumber.fromInt(0);


    /**
     * The MConnection that this result set is in.
     */
    private MConnection connection;

    /**
     * The MStatement that this result is from.
     */
    private MStatement statement;

    /**
     * SQL warnings for this result set.  Cleared each time a new row accessed.
     */
    private SQLWarning head_warning;

    /**
     * The current result_id for the information in the current result set.
     */
    private int result_id;

    /**
     * The array of ColumnDescription that describes each column in the result
     * set.
     */
    private ColumnDescription[] col_list;

    /**
     * The length of time it took to execute this query in ms.
     */
    private int query_time_ms;

    /**
     * The number of rows in the result set.
     */
    private int result_row_count;

    /**
     * The maximum row count as set in the Statement by the 'setMaxRows' method
     * or 0 if the max row count is not important.
     */
    private int max_row_count = Integer.MAX_VALUE;

    /**
     * The row number of the first row of the 'result_block'
     */
    private int block_top_row;

    /**
     * The number of rows in 'result_block'
     */
    private int block_row_count;

    /**
     * The number of rows to fetch each time we need to get rows from the
     * database.
     */
    private int fetch_size;

    /**
     * The Vector that contains the Objects downloaded into this result set.
     * It only contains the objects from the last block of rows downloaded.
     */
    private Vector result_block;

    /**
     * The real index of the result set we are currently at.
     */
    private int real_index = Integer.MAX_VALUE;

    /**
     * The offset into 'result_block' where 'real_index' is.  This is set up
     * by 'ensureIndexLoaded'.
     */
    private int real_index_offset = -1;

    /**
     * Set to true if the last 'getxxx' method was a null.  Otherwise set to
     * false.
     */
    private boolean last_was_null = false;

    /**
     * A Hashtable that acts as a cache for column name/column number look ups.
     */
    private Hashtable column_hash;

    /**
     * Set to true if the result set is closed on the server.
     */
    private boolean closed_on_server;


    /**
     * Constructor.
     */
    MResultSet(MConnection connection, MStatement statement) {
        this.connection = connection;
        this.statement = statement;
        /**
         * A unique int that refers to this result set.
         */
        int unique_id = unique_id_key++;
        result_id = -1;
        result_block = new Vector();
    }


    /**
     * Adds a new SQLWarning to the chain.
     */
    void addSQLWarning(SQLWarning warning) {
        if (head_warning == null) {
            head_warning = warning;
        } else {
            head_warning.setNextWarning(warning);
        }
    }

    /**
     * Returns true if verbose column names are enabled on the connection.
     * Returns false by default.
     */
    boolean verboseColumnNames() {
        return connection.verboseColumnNames();
    }

    // ---------- Connection callback methods ----------
    // These methods are called back from the ConnectionThread running on the
    // connection.  These methods require some synchronization thought.

    /**
     * Called by the ConnectionThread when we have received the initial bag of
     * the result set.  This contains information about the columns in the
     * result, the number of rows in the entire set, etc.  This sets up the
     * result set to handle the result.
     */
    void connSetup(int result_id, ColumnDescription[] col_list,
                   int total_row_count) {
        this.result_id = result_id;
        this.col_list = col_list;
        this.result_row_count = total_row_count;
        block_top_row = -1;
        result_block.removeAllElements();

        real_index = -1;
        fetch_size = DEFAULT_FETCH_SIZE;
        closed_on_server = false;
    }

    /**
     * Sets the length of time in milliseconds (server-side) it took to execute
     * this query.  Useful as feedback for the server-side optimisation systems.
     * <p>
     * VERY MINOR ISSUE: An int can 'only' contain 35 weeks worth of
     *   milliseconds.  So if a query takes longer than that this number will
     *   overflow.
     */
    void setQueryTime(int time_ms) {
        query_time_ms = time_ms;
    }

    /**
     * Sets the maximum number of rows that this ResultSet will return or 0 if
     * the max number of rows is not important.  This is set by MStatement
     * when a query is evaluated.
     */
    void setMaxRowCount(int row_count) {
        if (row_count == 0) {
            max_row_count = Integer.MAX_VALUE;
        } else {
            max_row_count = row_count;
        }
    }

    /**
     * Returns true if this ResultSet contains large objects.  This looks at the
     * ColumnDescription object to determine this.
     */
    boolean containsLargeObjects() {
        for (int i = 0; i < col_list.length; ++i) {
            ColumnDescription col = col_list[i];
            int sql_type = col.getSQLType();
            if (sql_type == com.pony.database.global.SQLTypes.BINARY ||
                    sql_type == com.pony.database.global.SQLTypes.VARBINARY ||
                    sql_type == com.pony.database.global.SQLTypes.LONGVARBINARY ||
                    sql_type == com.pony.database.global.SQLTypes.BLOB ||
                    sql_type == com.pony.database.global.SQLTypes.CHAR ||
                    sql_type == com.pony.database.global.SQLTypes.VARCHAR ||
                    sql_type == com.pony.database.global.SQLTypes.LONGVARCHAR ||
                    sql_type == com.pony.database.global.SQLTypes.CLOB) {
                return true;
            }
        }
        return false;
    }

    /**
     * Asks the server for all the rows in the result set and stores it
     * locally within this object.  It then disposes all resources associated
     * with this result set on the server.
     */
    void storeResultLocally() throws SQLException {
        // After this call, 'result_block' will contain the whole result set.
        updateResultPart(0, rowCount());
        // Request to close the current result set on the server.
        connection.disposeResult(result_id);
        closed_on_server = true;
    }

    /**
     * Asks the server for more information about this result set to put
     * into the 'result_block'.  This should be called when we need to request
     * more information from the server.
     * <p>
     * @param row_index the top row index from the block of the result set to
     *   download.
     * @param row_count the maximum number of rows to download (may be less if
     *   no more are available).
     */
    void updateResultPart(int row_index, int row_count) throws SQLException {

        // If row_count is 0 then we don't need to do anything.
        if (row_count == 0) {
            return;
        }

        if (row_index + row_count < 0) {
            throw new SQLException(
                    "ResultSet row index is before the start of the set.");
        } else if (row_index < 0) {
            row_index = 0;
            row_count = row_count + row_index;
        }

        if (row_index >= rowCount()) {
            throw new SQLException(
                    "ResultSet row index is after the end of the set.");
        } else if (row_index + row_count > rowCount()) {
            row_count = rowCount() - row_index;
        }

        if (result_id == -1) {
            throw new SQLException("result_id == -1.  No result to get from.");
        }

        try {

            // Request the result via the RowCache.  If the information is not found
            // in the row cache then the request is forwarded onto the database.
            result_block = connection.getRowCache().getResultPart(result_block,
                    connection, result_id, row_index, row_count,
                    columnCount(), rowCount());

            // Set the row that's at the top
            block_top_row = row_index;
            // Set the number of rows in the block.
            block_row_count = row_count;

//      // Request a part of a result from the server (blocks)
//      DataInputStream din = connection.requestResultPart(result_id,
//                                                      row_index, row_count);
//
//      // Clear the block.
//      result_block.removeAllElements();
//      int num_entries = row_count * columnCount();
//      for (int i = 0; i < num_entries; ++i) {
//        result_block.addElement(ObjectTransfer.readFrom(din));
//      }

        } catch (IOException e) {
            e.printStackTrace();
            throw new SQLException("IO Error: " + e.getMessage());
        }

    }

    /**
     * Closes the current server side result for this result set ready for a
     * new one.  This should be called before we execute a query.  It sends a
     * command to the server to dispose of any resources associated with the
     * current result_id.
     * <p>
     * It's perfectly safe to call this method even if we haven't downloaded
     * a result set from the server and you may also safely call it multiple
     * times (it will only send one request to the server).
     */
    void closeCurrentResult() throws SQLException {
        if (getResultID() != -1) {
            if (!closed_on_server) {
                // Request to close the current result set
                connection.disposeResult(result_id);
                closed_on_server = true;
            }
            result_id = -1;
            real_index = Integer.MAX_VALUE;
            // Clear the column name -> index mapping,
            if (column_hash != null) {
                column_hash.clear();
            }
        }
    }

    /**
     * Returns the 'result_id' that is used as a key to refer to the result set
     * on the server that is the result of the query.  A 'resultID' of -1 means
     * there is no server side result set associated with this object.
     */
    int getResultID() {
        return result_id;
    }

    /**
     * The total number of rows in the result set.
     */
    int rowCount() {
        // The row count is whatever is the least between max_row_count (the
        // maximum the user has set) and result_row_count (the actual number of
        // rows in the result.
        return Math.min(result_row_count, max_row_count);
    }

    /**
     * The column count of columns in the result set.
     */
    int columnCount() {
        return col_list.length;
    }

    /**
     * Returns the ColumnDescription of the given column (first column is 0,
     * etc).
     */
    ColumnDescription getColumn(int column) {
        return col_list[column];
    }

    /**
     * Returns true if this result set contains 1 column and 1 row and the name
     * of the column is 'result'.  This indicates the result set is a DDL
     * command ( UPDATE, INSERT, CREATE, ALTER, etc ).
     * <p>
     * <strong>NOTE:</strong> This is a minor hack because there is no real
     * indication that this is a DML statement.  Theoretically a DQL query could
     * be constructed that meets all these requirements and is processed
     * incorrectly.
     */
    boolean isUpdate() {
        // Must have 1 col and 1 row and the title of the column must be
        // 'result' aliased.
        return (columnCount() == 1 && rowCount() == 1 &&
                getColumn(0).getName().equals("@aresult"));
    }

    /**
     * Returns this ResultSet as an 'int' value.  This is only valid if the
     * result set has a single column and a single row of type 'BigNumber'.
     */
    int intValue() throws SQLException {
        if (isUpdate()) {
            Object ob = result_block.elementAt(0);
            if (ob instanceof BigNumber) {
                return ((BigNumber) ob).intValue();
            } else {
                return 0;
            }
        }
        throw new SQLException("Unable to format query result as an update value.");
    }

    /**
     * Disposes of all resources associated with this result set.  This could
     * either be called from the MStatement finalize or close method.  Calls to
     * this object are undefined after this method has finished.
     */
    void dispose() {
        try {
            close();
        } catch (SQLException e) {
            // Ignore
            // We ignore exceptions because handling cases where the server
            // connection has broken for many ResultSets would be annoying.
        }

        connection = null;
        statement = null;
        col_list = null;
        result_block = null;
    }

    /**
     * Ensures that the row index pointed to by 'real_index' is actually loaded
     * into the 'result_block'.  If not, we send a request to the database to
     * get it.
     */
    void ensureIndexLoaded() throws SQLException {
        if (real_index == -1) {
            throw new SQLException("Row index out of bounds.");
        }

        // Offset into our block
        int row_offset = real_index - block_top_row;
        if (row_offset >= block_row_count) {
            // Need to download the next block from the server.
            updateResultPart(real_index, fetch_size);
            // Set up the index into the downloaded block.
            row_offset = real_index - block_top_row;
            real_index_offset = row_offset * columnCount();
        } else if (row_offset < 0) {
            int fs_dif = Math.min(fetch_size, 8);
            // Need to download the next block from the server.
            updateResultPart(real_index - fetch_size + fs_dif, fetch_size);
            // Set up the index into the downloaded block.
            row_offset = real_index - block_top_row;
            real_index_offset = row_offset * columnCount();
        }
    }

    /**
     * Searches for the index of the column with the given name.  First column
     * index is 1, second is 2, etc.
     */
    int findColumnIndex(String name) throws SQLException {
        // For speed, we keep column name -> column index mapping in the hashtable.
        // This makes column reference by string faster.
        if (column_hash == null) {
            column_hash = new Hashtable();
        }

        boolean case_insensitive = connection.isCaseInsensitiveIdentifiers();
        if (case_insensitive) {
            name = name.toUpperCase();
        }

        Integer index = (Integer) column_hash.get(name);
        if (index == null) {
            int col_count = columnCount();
            // First construct an unquoted list of all column names
            String[] cols = new String[col_count];
            for (int i = 0; i < col_count; ++i) {
                String col_name = col_list[i].getName();
                if (col_name.startsWith("\"")) {
                    col_name = col_name.substring(1, col_name.length() - 1);
                }
                // Strip any codes from the name
                if (col_name.startsWith("@")) {
                    col_name = col_name.substring(2);
                }
                if (case_insensitive) {
                    col_name = col_name.toUpperCase();
                }
                cols[i] = col_name;
            }

            for (int i = 0; i < col_count; ++i) {
                String col_name = cols[i];
                if (col_name.equals(name)) {
                    column_hash.put(name, new Integer(i + 1));
                    return i + 1;
                }
            }

            // If not found then search for column name ending,
            String point_name = "." + name;
            for (int i = 0; i < col_count; ++i) {
                String col_name = cols[i];
                if (col_name.endsWith(point_name)) {
                    column_hash.put(name, new Integer(i + 1));
                    return i + 1;
                }
            }

//      // DEBUG: output the list of columns,
//      for (int i = 0; i < col_count; ++i) {
//        System.out.println(cols[i]);
//      }
            throw new SQLException("Couldn't find column with name: " + name);
        } else {
            return index.intValue();
        }
    }

    /**
     * Returns the column Object of the current index.  The first column is 1,
     * the second is 2, etc.
     */
    Object getRawColumn(int column) throws SQLException {
        // ASSERTION -
        // Is the given column in bounds?
        if (column < 1 || column > columnCount()) {
            throw new SQLException(
                    "Column index out of bounds: 1 > " + column + " > " + columnCount());
        }
        // Ensure the current indexed row is fetched from the server.
        ensureIndexLoaded();
        // Return the raw cell object.
        Object ob = result_block.elementAt(real_index_offset + (column - 1));
        // Null check of the returned object,
        if (ob != null) {
            last_was_null = false;
            // If this is a java object then deserialize it,
            // ISSUE: Cache deserialized objects?
            if (getColumn(column - 1).getSQLType() ==
                    com.pony.database.global.SQLTypes.JAVA_OBJECT) {
                ob = ObjectTranslator.deserialize((ByteLongObject) ob);
            }
            return ob;
        }
        last_was_null = true;
        return null;
    }

    /**
     * Returns the column Object of the name of the current index.
     */
    Object getRawColumn(String name) throws SQLException {
        return getRawColumn(findColumnIndex(name));
    }

    /**
     * This should be called when the 'real_index' variable changes.  It updates
     * internal variables.
     */
    private void realIndexUpdate() throws SQLException {
        // Set up the index into the downloaded block.
        int row_offset = real_index - block_top_row;
        real_index_offset = row_offset * columnCount();
        // Clear any warnings as in the spec.
        clearWarnings();
    }

    /**
     * Returns true if the given object is either an instanceof StringObject or
     * is an instanceof StreamableObject, and therefore can be made into a
     * string.
     */
    private boolean canMakeString(Object ob) {
        return (ob instanceof StringObject || ob instanceof StreamableObject);
    }

    /**
     * If the object represents a String or is a form that can be readily
     * translated to a String (such as a Clob, String, BigNumber, Boolean, etc)
     * the string representation of the given Object is returned.  This method is
     * a convenient way to convert objects such as Clobs into java.util.String
     * objects.  This will cause a ClassCastException if the given object
     * represents a BLOB or ByteLongObject.
     */
    private String makeString(Object ob) throws SQLException {
        if (ob instanceof StreamableObject) {
            Clob clob = asClob(ob);
            long clob_len = clob.length();
            if (clob_len < 16384L * 65536L) {
                return clob.getSubString(1, (int) clob_len);
            }
            throw new SQLException("Clob too large to return as a string.");
        } else if (ob instanceof ByteLongObject) {
            throw new ClassCastException();
        } else {
            return ob.toString();
        }
    }

    /**
     * Returns the given object as a Blob instance.
     */
    private Blob asBlob(Object ob) {
        if (ob instanceof StreamableObject) {
            StreamableObject s_ob = (StreamableObject) ob;
            byte type = (byte) (s_ob.getType() & 0x0F);
            if (type == 2) {
                return new MStreamableBlob(connection, result_id, type,
                        s_ob.getIdentifier(), s_ob.getSize());
            }
        } else if (ob instanceof ByteLongObject) {
            return new MBlob((ByteLongObject) ob);
        }
        throw new ClassCastException();
    }

    /**
     * Returns the given object as a Clob instance.
     */
    private Clob asClob(Object ob) {
        if (ob instanceof StreamableObject) {
            StreamableObject s_ob = (StreamableObject) ob;
            byte type = (byte) (s_ob.getType() & 0x0F);
            if (type == 3 ||
                    type == 4) {
                return new MStreamableClob(connection, result_id, type,
                        s_ob.getIdentifier(), s_ob.getSize());
            }
        } else if (ob instanceof StringObject) {
            return new MClob(ob.toString());
        }
        throw new ClassCastException();
    }

    /**
     * Casts an internal object to the sql_type given for return by methods
     * such as 'getObject'.
     */
    private Object jdbcObjectCast(Object ob, int sql_type) throws SQLException {
        switch (sql_type) {
            case (SQLTypes.BIT):
                return ob;
            case (SQLTypes.TINYINT):
                return new Byte(((BigNumber) ob).byteValue());
            case (SQLTypes.SMALLINT):
                return new Short(((BigNumber) ob).shortValue());
            case (SQLTypes.INTEGER):
                return new Integer(((BigNumber) ob).intValue());
            case (SQLTypes.BIGINT):
                return new Long(((BigNumber) ob).longValue());
            case (SQLTypes.FLOAT):
                return new Double(((BigNumber) ob).doubleValue());
            case (SQLTypes.REAL):
                return new Float(((BigNumber) ob).floatValue());
            case (SQLTypes.DOUBLE):
                return new Double(((BigNumber) ob).doubleValue());
            case (SQLTypes.NUMERIC):
                return ((BigNumber) ob).asBigDecimal();
            case (SQLTypes.DECIMAL):
                return ((BigNumber) ob).asBigDecimal();
            case (SQLTypes.CHAR):
                return makeString(ob);
            case (SQLTypes.VARCHAR):
                return makeString(ob);
            case (SQLTypes.LONGVARCHAR):
                return makeString(ob);
            case (SQLTypes.DATE):
                return new java.sql.Date(((java.util.Date) ob).getTime());
            case (SQLTypes.TIME):
                return new java.sql.Time(((java.util.Date) ob).getTime());
            case (SQLTypes.TIMESTAMP):
                return new java.sql.Timestamp(((java.util.Date) ob).getTime());
            case (SQLTypes.BINARY):
                // fall through
            case (SQLTypes.VARBINARY):
                // fall through
            case (SQLTypes.LONGVARBINARY):
                Blob b = asBlob(ob);
                return b.getBytes(1, (int) b.length());
            case (SQLTypes.NULL):
                return ob;
            case (SQLTypes.OTHER):
                return ob;
            case (SQLTypes.JAVA_OBJECT):
                return ob;
            case (SQLTypes.DISTINCT):
                // (Not supported)
                return ob;
            case (SQLTypes.STRUCT):
                // (Not supported)
                return ob;
            case (SQLTypes.ARRAY):
                // (Not supported)
                return ob;
//#IFDEF(JDBC2.0)
            case (SQLTypes.BLOB):
                return asBlob(ob);
            case (SQLTypes.CLOB):
                return asClob(ob);
            case (SQLTypes.REF):
                // (Not supported)
                return ob;
//#ENDIF
            default:
                return ob;
        }
    }

    // ---------- JDBC Extentions ----------
    // All non-standard extentions to the JDBC API.  This is rather ugly because
    // to use these we need to cast to a com.pony.database.jdbc.? class.

    /**
     * The number of milliseconds it took the server to execute this query.
     * This is set after the call to 'connSetup' so is available as soon as the
     * header information is received from the server.
     */
    public int extQueryTimeMillis() {
        return query_time_ms;
    }

    /**
     * Access column as java.util.Date (which is the native object used in the
     * pony database to handle time).
     */
    public java.util.Date extGetDate(int columnIndex) throws SQLException {
        return (java.util.Date) getRawColumn(columnIndex);
    }

    /**
     * Access column as java.util.Date (which is the native object used in the
     * pony database to handle time).
     */
    public java.util.Date extGetDate(String columnName) throws SQLException {
        return extGetDate(findColumnIndex(columnName));
    }

    // ---------- Implemented from ResultSet ----------

    public boolean next() throws SQLException {
        int row_count = rowCount();
        if (real_index < row_count) {
            ++real_index;
            if (real_index < row_count) {
                realIndexUpdate();
            }
        }
        return (real_index < row_count);
    }

    public void close() throws SQLException {
        closeCurrentResult();
    }

    public boolean wasNull() throws SQLException {
        // Note: we don't check that a previous value was read.
        return last_was_null;
    }

    //======================================================================
    // Methods for accessing results by column index
    //======================================================================

    public String getString(int columnIndex) throws SQLException {
        Object str = getRawColumn(columnIndex);
        if (str == null) {
            return null;
        } else {
            if (canMakeString(str)) {
                return makeString(str);
            } else {
                // For date, time and timestamp we must format as per the JDBC
                // specification.
                if (str instanceof java.util.Date) {
                    int sql_type = getColumn(columnIndex - 1).getSQLType();
                    return jdbcObjectCast(str, sql_type).toString();
                }
                return str.toString();
            }
        }
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        Object ob = getRawColumn(columnIndex);
        if (ob == null) {
            return false;
        } else if (ob instanceof Boolean) {
            return ((Boolean) ob).booleanValue();
        } else if (ob instanceof BigNumber) {
            return ((BigNumber) ob).compareTo(BD_ZERO) != 0;
        } else if (canMakeString(ob)) {
            return makeString(ob).equalsIgnoreCase("true");
        } else {
            throw new SQLException("Unable to cast value in ResultSet to boolean");
        }
    }

    public byte getByte(int columnIndex) throws SQLException {
        // Translates from BigNumber
        BigNumber num = getBigNumber(columnIndex);
        return num == null ? 0 : num.byteValue();
    }

    public short getShort(int columnIndex) throws SQLException {
        // Translates from BigNumber
        BigNumber num = getBigNumber(columnIndex);
        return num == null ? 0 : num.shortValue();
    }

    public int getInt(int columnIndex) throws SQLException {
        // Translates from BigNumber
        BigNumber num = getBigNumber(columnIndex);
        return num == null ? 0 : num.intValue();
    }

    public long getLong(int columnIndex) throws SQLException {
        // Translates from BigNumber
        BigNumber num = getBigNumber(columnIndex);
        return num == null ? 0 : num.longValue();
    }

    public float getFloat(int columnIndex) throws SQLException {
        // Translates from BigNumber
        BigNumber num = getBigNumber(columnIndex);
        return num == null ? 0 : num.floatValue();
    }

    public double getDouble(int columnIndex) throws SQLException {
        // Translates from BigNumber
        BigNumber num = getBigNumber(columnIndex);
        return num == null ? 0 : num.doubleValue();
    }

    /**
     * @deprecated
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException {
        return getBigDecimal(columnIndex);
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        Blob b = getBlob(columnIndex);
        if (b == null) {
            return null;
        } else {
            if (b.length() <= Integer.MAX_VALUE) {
                return b.getBytes(1, (int) b.length());
            } else {
                throw new SQLException("Blob too large to return as byte[]");
            }
        }
//    Object ob = getRawColumn(columnIndex);
//    if (ob == null) {
//      return null;
//    }
//    else if (ob instanceof ByteLongObject) {
//      // Return a safe copy of the byte[] array (BLOB).
//      ByteLongObject b = (ByteLongObject) ob;
//      byte[] barr = new byte[b.length()];
//      System.arraycopy(b.getByteArray(), 0, barr, 0, b.length());
//      return barr;
//    }
//    else {
//      throw new SQLException("Unable to cast value in ResultSet to byte[]");
//    }
    }

    public Date getDate(int columnIndex) throws SQLException {
        // Wrap java.util.Date with java.sql.Date
        java.util.Date d = extGetDate(columnIndex);
        if (d != null) {
            return new Date(d.getTime());
        }
        return null;
    }

    public java.sql.Time getTime(int columnIndex) throws SQLException {
        // Wrap java.util.Date with java.sql.Time
        java.util.Date d = extGetDate(columnIndex);
        if (d != null) {
            return new Time(d.getTime());
        }
        return null;
    }

    public java.sql.Timestamp getTimestamp(int columnIndex)
            throws SQLException {
        // ISSUE: This may be incorrectly implemented....
        // Wrap java.util.Date with java.sql.Timestamp
        java.util.Date d = extGetDate(columnIndex);
        if (d != null) {
            return new Timestamp(d.getTime());
        }
        return null;
    }


    public java.io.InputStream getAsciiStream(int columnIndex)
            throws SQLException {
        Clob c = getClob(columnIndex);
        if (c == null) {
            return null;
        } else {
            return c.getAsciiStream();
        }
    }

    /**
     * @deprecated
     */
    public java.io.InputStream getUnicodeStream(int columnIndex)
            throws SQLException {
        throw new SQLException("Deprecated method not supported");
    }

    public java.io.InputStream getBinaryStream(int columnIndex)
            throws SQLException {
        Blob blob = getBlob(columnIndex);
        if (blob == null) {
            return null;
        } else {
            return blob.getBinaryStream();
        }
//    Object ob = getRawColumn(columnIndex);
//    if (ob == null) {
//      return null;
//    }
//    else if (ob instanceof ByteLongObject) {
//      ByteLongObject b = (ByteLongObject) ob;
//      return new ByteArrayInputStream(b.getByteArray());
//    }
//    else {
//      throw new SQLException(
//                      "Unable to cast value in ResultSet to binary stream");
//    }
    }

    //======================================================================
    // Methods for accessing results by column name
    //======================================================================

    public String getString(String columnName) throws SQLException {
        return getString(findColumnIndex(columnName));
    }

    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumnIndex(columnName));
    }

    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumnIndex(columnName));
    }

    public short getShort(String columnName) throws SQLException {
        return getShort(findColumnIndex(columnName));
    }

    public int getInt(String columnName) throws SQLException {
        return getInt(findColumnIndex(columnName));
    }

    public long getLong(String columnName) throws SQLException {
        return getLong(findColumnIndex(columnName));
    }

    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumnIndex(columnName));
    }

    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumnIndex(columnName));
    }

    /**
     * @deprecated
     */
    public BigDecimal getBigDecimal(String columnName, int scale)
            throws SQLException {
        return getBigDecimal(findColumnIndex(columnName));
    }

    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumnIndex(columnName));
    }

    public java.sql.Date getDate(String columnName) throws SQLException {
        return getDate(findColumnIndex(columnName));
    }

    public java.sql.Time getTime(String columnName) throws SQLException {
        return getTime(findColumnIndex(columnName));
    }

    public java.sql.Timestamp getTimestamp(String columnName)
            throws SQLException {
        return getTimestamp(findColumnIndex(columnName));
    }

    public java.io.InputStream getAsciiStream(String columnName)
            throws SQLException {
        return getAsciiStream(findColumnIndex(columnName));
    }

    /**
     * @deprecated
     */
    public java.io.InputStream getUnicodeStream(String columnName)
            throws SQLException {
        return getUnicodeStream(findColumnIndex(columnName));
    }

    public java.io.InputStream getBinaryStream(String columnName)
            throws SQLException {
        return getBinaryStream(findColumnIndex(columnName));
    }

    //=====================================================================
    // Advanced features:
    //=====================================================================

    public SQLWarning getWarnings() throws SQLException {
        return head_warning;
    }

    public void clearWarnings() throws SQLException {
        head_warning = null;
    }

    public String getCursorName() throws SQLException {
        // Cursor not supported...
        throw MSQLException.unsupported();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return new MResultSetMetaData(this);
    }

    public Object getObject(int columnIndex) throws SQLException {
        Object ob = getRawColumn(columnIndex);
        if (ob == null) {
            return ob;
        }
        if (connection.isStrictGetObject()) {
            // Convert depending on the column type,
            ColumnDescription col_desc = getColumn(columnIndex - 1);
            int sql_type = col_desc.getSQLType();

            return jdbcObjectCast(ob, sql_type);

        }
//#IFDEF(JDBC2.0)
        else {
            // For blobs, return an instance of Blob.
            if (ob instanceof ByteLongObject ||
                    ob instanceof StreamableObject) {
                return asBlob(ob);
            }
        }
//#ENDIF
        return ob;
    }

    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumnIndex(columnName));
    }

    //----------------------------------------------------------------

    public int findColumn(String columnName) throws SQLException {
        return findColumnIndex(columnName);
    }

    //--------------------------JDBC 2.0-----------------------------------

    // NOTE: We allow 'getBigDecimal' methods as extensions to JDBC 1.0
    //   because they are a key object in the Pony world.

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        BigNumber bnum = getBigNumber(columnIndex);
        if (bnum != null) {
            return bnum.asBigDecimal();
        } else {
            return null;
        }
    }

    private BigNumber getBigNumber(int columnIndex) throws SQLException {
        Object ob = getRawColumn(columnIndex);
        if (ob == null) {
            return null;
        }
        if (ob instanceof BigNumber) {
            return (BigNumber) ob;
        } else {
            return BigNumber.fromString(makeString(ob));
        }
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(findColumnIndex(columnName));
    }

    // NOTE: We allow 'setFetchSize' and 'getFetchSize' as extensions to
    //   JDBC 1.0 also.

    public void setFetchSize(int rows) throws SQLException {
        if (rows > 0) {
            fetch_size = Math.min(rows, MAXIMUM_FETCH_SIZE);
        } else {
            fetch_size = DEFAULT_FETCH_SIZE;
        }
    }

    public int getFetchSize() throws SQLException {
        return fetch_size;
    }

//#IFDEF(JDBC2.0)

    //---------------------------------------------------------------------
    // Getters and Setters
    //---------------------------------------------------------------------

    public java.io.Reader getCharacterStream(int columnIndex)
            throws SQLException {
        Clob c = getClob(columnIndex);
        if (c == null) {
            return null;
        } else {
            return c.getCharacterStream();
        }
    }

    public java.io.Reader getCharacterStream(String columnName)
            throws SQLException {
        return getCharacterStream(findColumnIndex(columnName));
    }


    //---------------------------------------------------------------------
    // Traversal/Positioning
    //---------------------------------------------------------------------

    public boolean isBeforeFirst() throws SQLException {
        return real_index < 0;
    }

    public boolean isAfterLast() throws SQLException {
        return real_index >= rowCount();
    }

    public boolean isFirst() throws SQLException {
        return real_index == 0;
    }

    public boolean isLast() throws SQLException {
        return real_index == rowCount() - 1;
    }

    public void beforeFirst() throws SQLException {
        real_index = -1;
    }

    public void afterLast() throws SQLException {
        real_index = rowCount();
    }

    public boolean first() throws SQLException {
        real_index = 0;
        realIndexUpdate();
        return real_index < rowCount();
    }

    public boolean last() throws SQLException {
        real_index = rowCount() - 1;
        realIndexUpdate();
        return real_index >= 0;
    }

    public int getRow() throws SQLException {
        return real_index + 1;
    }

    public boolean absolute(int row) throws SQLException {
        if (row > 0) {
            real_index = row - 1;
        } else if (row < 0) {
            real_index = rowCount() + row;
        }
        realIndexUpdate();

        return (real_index >= 0 && real_index < rowCount());
    }

    public boolean relative(int rows) throws SQLException {
        real_index += rows;

        int row_count = rowCount();
        if (real_index < -1) {
            real_index = -1;
        }
        if (real_index > row_count) {
            real_index = row_count;
        }
        realIndexUpdate();

        return (real_index >= 0 && real_index < rowCount());
    }

    public boolean previous() throws SQLException {
        if (real_index >= 0) {
            --real_index;
            realIndexUpdate();
        }

        return real_index >= 0;
    }

    public void setFetchDirection(int direction) throws SQLException {
        // Currently ignored...
        // We could improve cache performance with this hint.
    }

    public int getFetchDirection() throws SQLException {
        // Return default...
        // We could improve cache performance with this hint.
        return FETCH_UNKNOWN;
    }

    public int getType() throws SQLException {
        // Supports scrolling but can't change
        return TYPE_SCROLL_INSENSITIVE;
    }

    public int getConcurrency() throws SQLException {
        // Only support read only result sets...
        return CONCUR_READ_ONLY;
    }

    //---------------------------------------------------------------------
    // Updates
    //---------------------------------------------------------------------

    public boolean rowUpdated() throws SQLException {
        throw MSQLException.unsupported();
    }

    public boolean rowInserted() throws SQLException {
        throw MSQLException.unsupported();
    }

    public boolean rowDeleted() throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateDate(int columnIndex, java.sql.Date x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateTime(int columnIndex, java.sql.Time x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateTimestamp(int columnIndex, java.sql.Timestamp x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateAsciiStream(int columnIndex,
                                  java.io.InputStream x,
                                  int length) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBinaryStream(int columnIndex,
                                   java.io.InputStream x,
                                   int length) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateCharacterStream(int columnIndex,
                                      java.io.Reader x,
                                      int length) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateObject(int columnIndex, Object x, int scale)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateNull(String columnName) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBoolean(String columnName, boolean x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateShort(String columnName, short x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateInt(String columnName, int x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateLong(String columnName, long x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBigDecimal(String columnName, BigDecimal x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateString(String columnName, String x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateDate(String columnName, java.sql.Date x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateTime(String columnName, java.sql.Time x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateTimestamp(String columnName, java.sql.Timestamp x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateAsciiStream(String columnName,
                                  java.io.InputStream x,
                                  int length) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBinaryStream(String columnName,
                                   java.io.InputStream x,
                                   int length) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateCharacterStream(String columnName,
                                      java.io.Reader reader,
                                      int length) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateObject(String columnName, Object x, int scale)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void insertRow() throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateRow() throws SQLException {
        throw MSQLException.unsupported();
    }

    public void deleteRow() throws SQLException {
        throw MSQLException.unsupported();
    }

    public void refreshRow() throws SQLException {
        throw MSQLException.unsupported();
    }

    public void cancelRowUpdates() throws SQLException {
        throw MSQLException.unsupported();
    }

    public void moveToInsertRow() throws SQLException {
        throw MSQLException.unsupported();
    }

    public void moveToCurrentRow() throws SQLException {
        throw MSQLException.unsupported();
    }

    public Statement getStatement() throws SQLException {
        return statement;
    }

    public Object getObject(int i, java.util.Map map) throws SQLException {
        // Haven't had time to research what exactly needs to be stored in the
        // map, so I'm defaulting this to 'getObject'
        return getObject(i);
    }

    public Ref getRef(int i) throws SQLException {
        // Interesting idea this...  Can't really see the applications of it
        // though unless you dealing with a really big cell and you just want to
        // pass around the reference rather than the actual cell contents.
        // Easy to fudge an implementation for this if an application needs it.
        throw MSQLException.unsupported();
    }

    public Blob getBlob(int i) throws SQLException {
        // I'm assuming we must return 'null' for a null blob....
        Object ob = getRawColumn(i);
        if (ob != null) {
            try {
                return asBlob(ob);
            } catch (ClassCastException e) {
                throw new SQLException("Column " + i + " is not a binary column.");
            }
        }
        return null;
    }

    public Clob getClob(int i) throws SQLException {
        // I'm assuming we must return 'null' for a null clob....
        Object ob = getRawColumn(i);
        if (ob != null) {
            try {
                return asClob(ob);
            } catch (ClassCastException e) {
                throw new SQLException("Column " + i + " is not a character column.");
            }
        }
        return null;
    }

    public Array getArray(int i) throws SQLException {
        // Arrays not available in database...
        throw MSQLException.unsupported();
    }

    public Object getObject(String colName, java.util.Map map)
            throws SQLException {
        // Haven't had time to research what exactly needs to be stored in the
        // map, so I'm defaulting this to 'getObject'
        return getObject(colName);
    }

    public Ref getRef(String colName) throws SQLException {
        throw MSQLException.unsupported();
    }

    public Blob getBlob(String colName) throws SQLException {
        return getBlob(findColumnIndex(colName));
    }

    public Clob getClob(String colName) throws SQLException {
        return getClob(findColumnIndex(colName));
    }

    public Array getArray(String colName) throws SQLException {
        throw MSQLException.unsupported();
    }

    public java.sql.Date getDate(int columnIndex, Calendar cal)
            throws SQLException {
        return getDate(columnIndex);
    }

    public java.sql.Date getDate(String columnName, Calendar cal)
            throws SQLException {
        return getDate(columnName);
    }

    public java.sql.Time getTime(int columnIndex, Calendar cal)
            throws SQLException {
        return getTime(columnIndex);
    }

    public java.sql.Time getTime(String columnName, Calendar cal)
            throws SQLException {
        return getTime(columnName);
    }

    public java.sql.Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException {
        return getTimestamp(columnIndex);
    }

    public java.sql.Timestamp getTimestamp(String columnName, Calendar cal)
            throws SQLException {
        return getTimestamp(columnName);
    }

//#ENDIF

//#IFDEF(JDBC3.0)

    //-------------------------- JDBC 3.0 ----------------------------------------

    public java.net.URL getURL(int columnIndex) throws SQLException {
        throw MSQLException.unsupported();
    }

    public java.net.URL getURL(String columnName) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateRef(int columnIndex, java.sql.Ref x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateRef(String columnName, java.sql.Ref x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBlob(int columnIndex, java.sql.Blob x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateBlob(String columnName, java.sql.Blob x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateClob(int columnIndex, java.sql.Clob x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateClob(String columnName, java.sql.Clob x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateArray(int columnIndex, java.sql.Array x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public void updateArray(String columnName, java.sql.Array x)
            throws SQLException {
        throw MSQLException.unsupported();
    }

//#ENDIF

//#IFDEF(JDBC4.0)

    // -------------------------- JDK 1.6 -----------------------------------

    public RowId getRowId(int columnIndex) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public boolean isClosed() throws SQLException {
        return getResultID() == -1;
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public String getNString(int columnIndex) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public String getNString(String columnLabel) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public Object unwrap(Class iface) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public boolean isWrapperFor(Class iface) throws SQLException {
        throw MSQLException.unsupported16();
    }

//#ENDIF

//#IFDEF(JDBC5.0)

    // -------------------------- JDK 1.7 -----------------------------------

    public Object getObject(int columnIndex, Class type) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public Object getObject(String columnLabel, Class type) throws SQLException {
        throw MSQLException.unsupported16();
    }

//#ENDIF

    // ---------- Finalize ----------

    public void finalize() {
        dispose();
    }

}
