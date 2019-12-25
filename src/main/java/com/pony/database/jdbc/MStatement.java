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

import com.pony.database.global.StreamableObject;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of JDBC Statement.
 * <p>
 * Multi-threaded issue:  This class is not designed to be multi-thread
 *  safe.  A Statement should not be accessed by concurrent threads.
 *
 * @author Tobias Downer
 */

class MStatement implements Statement {

    /**
     * The MConnection object for this statement.
     */
    private final MConnection connection;

    /**
     * The list of all MResultSet objects that represents the results of a query.
     */
    private MResultSet[] result_set_list;


    private int max_field_size;
    private int max_row_count;
    private int query_timeout;
    private int fetch_size;

    private SQLWarning head_warning;

    private boolean escape_processing;

    /**
     * The list of queries to execute in a batch.
     */
    private List batch_list;

    /**
     * The list of streamable objects created via the 'createStreamableObject'
     * method.
     */
    private List streamable_object_list;

    /**
     * For multiple result sets, the index of the result set we are currently on.
     */
    private int multi_result_set_index;

    /**
     * Constructs the statement.
     */
    MStatement(MConnection connection) {
        this.connection = connection;
        this.escape_processing = true;
    }

    /**
     * Adds a new SQLWarning to the chain.
     */
    final void addSQLWarning(SQLWarning warning) {
        if (head_warning == null) {
            head_warning = warning;
        } else {
            head_warning.setNextWarning(warning);
        }
    }

    /**
     * Returns an array of ResultSet objects of the give length for this
     * statement.  This is intended for multiple result queries (such as batch
     * statements).
     */
    final MResultSet[] internalResultSetList(int count) {
        if (count <= 0) {
            throw new Error("'count' must be > 0");
        }

        if (result_set_list != null && result_set_list.length != count) {
            // Dispose all the ResultSet objects currently open.
            for (MResultSet mResultSet : result_set_list) {
                mResultSet.dispose();
            }
            result_set_list = null;
        }

        if (result_set_list == null) {
            result_set_list = new MResultSet[count];
            for (int i = 0; i < count; ++i) {
                result_set_list[i] = new MResultSet(connection, this);
            }
        }

        return result_set_list;
    }

    /**
     * Returns the single ResultSet object for this statement.  This should only
     * be used for single result queries.
     */
    final MResultSet internalResultSet() {
        return internalResultSetList(1)[0];
    }

    /**
     * Generates a new StreamableObject and stores it in the hold for future
     * access by the server.
     */
    protected StreamableObject createStreamableObject(InputStream x,
                                                      int length, byte type) {
        StreamableObject s_ob = connection.createStreamableObject(x, length, type);
        if (streamable_object_list == null) {
            streamable_object_list = new ArrayList();
        }
        streamable_object_list.add(s_ob);
        return s_ob;
    }

    /**
     * Adds a query to the batch of queries executed by this statement.
     */
    protected void addBatch(SQLQuery query) {
        if (batch_list == null) {
            batch_list = new ArrayList();
        }
        batch_list.add(query);
    }

    /**
     * Executes the given SQLQuery object and fill's in at most the top 10
     * entries of the result set.
     */
    protected MResultSet executeQuery(SQLQuery query) throws SQLException {
        // Get the local result set
        MResultSet result_set = internalResultSet();
        // Execute the query
        executeQueries(new SQLQuery[]{query});
        // Return the result set
        return result_set;
    }

    /**
     * Executes a batch of SQL queries as listed as an array.
     */
    protected MResultSet[] executeQueries(SQLQuery[] queries)
            throws SQLException {

        // Allocate the result set for this batch
        MResultSet[] results = internalResultSetList(queries.length);

        // Reset the result set index
        multi_result_set_index = 0;

        // For each query,
        for (int i = 0; i < queries.length; ++i) {
            // Prepare the query
            queries[i].prepare(escape_processing);
            // Make sure the result set is closed
            results[i].closeCurrentResult();
        }

        // Execute each query
        connection.executeQueries(queries, results);

        // Post processing on the ResultSet objects
        for (int i = 0; i < queries.length; ++i) {
            MResultSet result_set = results[i];
            // Set the fetch size
            result_set.setFetchSize(fetch_size);
            // Set the max row count
            result_set.setMaxRowCount(max_row_count);
            // Does the result set contain large objects?  We can't cache a
            // result that contains binary data.
            boolean contains_large_objects = result_set.containsLargeObjects();
            // If the result row count < 40 then download and store locally in the
            // result set and dispose the resources on the server.
            if (!contains_large_objects && result_set.rowCount() < 40) {
                result_set.storeResultLocally();
            } else {
                result_set.updateResultPart(0, Math.min(10, result_set.rowCount()));
            }
        }

        return results;

    }

    // ---------- Implemented from Statement ----------

    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(new SQLQuery(sql));
    }

    public int executeUpdate(String sql) throws SQLException {
        MResultSet result_set = executeQuery(new SQLQuery(sql));
        return result_set.intValue();  // Throws SQL error if not 1 col 1 row
    }

    public void close() throws SQLException {
        // Behaviour of calls to Statement undefined after this method finishes.
        if (result_set_list != null) {
            for (MResultSet mResultSet : result_set_list) {
                mResultSet.dispose();
            }
            result_set_list = null;
        }
        // Remove any streamable objects that have been created on the client
        // side.
        if (streamable_object_list != null) {
            int sz = streamable_object_list.size();
            for (Object o : streamable_object_list) {
                StreamableObject s_object =
                        (StreamableObject) o;
                connection.removeStreamableObject(s_object);
            }
            streamable_object_list = null;
        }
    }

    //----------------------------------------------------------------------

    public int getMaxFieldSize() throws SQLException {
        // Are there limitations here?  Strings can be any size...
        return max_field_size;
    }

    public void setMaxFieldSize(int max) throws SQLException {
        if (max >= 0) {
            max_field_size = max;
        } else {
            throw new SQLException("MaxFieldSize negative.");
        }
    }

    public int getMaxRows() throws SQLException {
        return max_row_count;
    }

    public void setMaxRows(int max) throws SQLException {
        if (max >= 0) {
            max_row_count = max;
        } else {
            throw new SQLException("MaxRows negative.");
        }
    }

    public void setEscapeProcessing(boolean enable) throws SQLException {
        escape_processing = enable;
    }

    public int getQueryTimeout() throws SQLException {
        return query_timeout;
    }

    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds >= 0) {
            query_timeout = seconds;
            // Hack: We set the global query timeout for the driver in this VM.
            //   This global value is used in RemoteDatabaseInterface.
            //
            //   This is a nasty 'global change' hack.  A developer may wish to
            //   set a long timeout for one statement and a short for a different
            //   one however the timeout for all queries will be the very last time
            //   out set by any statement.  Unfortunately to fix this problem, we'll
            //   need to make a revision to the DatabaseInterface interface.   I
            //   don't think this is worth doing because I don't see this as being a
            //   major limitation of the driver.
            MDriver.QUERY_TIMEOUT = seconds;
        } else {
            throw new SQLException("Negative query timout.");
        }
    }

    public void cancel() throws SQLException {
        if (result_set_list != null) {
            for (MResultSet mResultSet : result_set_list) {
                connection.disposeResult(mResultSet.getResultID());
            }
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        return head_warning;
    }

    public void clearWarnings() throws SQLException {
        head_warning = null;
    }

    public void setCursorName(String name) throws SQLException {
        // Cursors not supported...
    }

    //----------------------- Multiple Results --------------------------

    // NOTE: Pony database doesn't support multiple result sets.  I think multi-
    //   result sets are pretty nasty anyway - are they really necessary?
    //   We do support the 'Multiple Results' interface for 1 result set.

    public boolean execute(String sql) throws SQLException {
        MResultSet result_set = executeQuery(new SQLQuery(sql));
        return !result_set.isUpdate();
    }

    public ResultSet getResultSet() throws SQLException {
        if (result_set_list != null) {
            if (multi_result_set_index < result_set_list.length) {
                return result_set_list[multi_result_set_index];
            }
        }
        return null;
    }

    public int getUpdateCount() throws SQLException {
        if (result_set_list != null) {
            if (multi_result_set_index < result_set_list.length) {
                MResultSet rs = result_set_list[multi_result_set_index];
                if (rs.isUpdate()) {
                    return rs.intValue();
                }
            }
        }
        return -1;
    }

    public boolean getMoreResults() throws SQLException {
        // If we are at the end then return false
        if (result_set_list == null ||
                multi_result_set_index >= result_set_list.length) {
            return false;
        }

        // Move to the next result set.
        ++multi_result_set_index;

        // We successfully moved to the next result
        return true;
    }

    //--------------------------JDBC 2.0-----------------------------

    // NOTE: These methods are provided as extensions for the JDBC 1.0 driver.

    public void setFetchSize(int rows) throws SQLException {
        if (rows >= 0) {
            fetch_size = rows;
        } else {
            throw new SQLException("Negative fetch size.");
        }
    }

    public int getFetchSize() throws SQLException {
        return fetch_size;
    }

//#IFDEF(JDBC2.0)

    public void setFetchDirection(int direction) throws SQLException {
        // We could use this hint to improve cache hits.....
    }

    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    public int getResultSetConcurrency() throws SQLException {
        // Read only I'm afraid...
        return ResultSet.CONCUR_READ_ONLY;
    }

    public int getResultSetType() throws SQLException {
        // Scroll insensitive operation only...
        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    public void addBatch(String sql) throws SQLException {
        addBatch(new SQLQuery(sql));
    }

    public void clearBatch() throws SQLException {
        batch_list = null;
    }

    public int[] executeBatch() throws SQLException {
        // Execute the batch,
        if (batch_list == null) {
            // Batch list is empty - nothing to do
            throw new SQLException("Batch list is empty - nothing to do.");
        }

        int sz = batch_list.size();
        SQLQuery[] batch_query_list = new SQLQuery[sz];
        for (int i = 0; i < sz; ++i) {
            batch_query_list[i] = (SQLQuery) batch_list.get(i);
        }
        try {
            // Execute the batch and find the results in the resultant array
            MResultSet[] batch_results = executeQueries(batch_query_list);

            // Put the result into an update array
            int[] update_result = new int[sz];
            for (int i = 0; i < sz; ++i) {
                update_result[i] = batch_results[i].intValue();
                batch_results[i].closeCurrentResult();
            }

            return update_result;
        } finally {
            // Make sure we clear the batch list.
            clearBatch();
        }
    }

    public Connection getConnection() throws SQLException {
        return connection;
    }

//#ENDIF

//#IFDEF(JDBC3.0)

    //--------------------------JDBC 3.0-----------------------------

    public boolean getMoreResults(int current) throws SQLException {
        return getMoreResults();
    }

    public ResultSet getGeneratedKeys() throws SQLException {
        throw MSQLException.unsupported();
    }

    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public boolean execute(String sql, int[] columnIndexes)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public int getResultSetHoldability() throws SQLException {
        // In Pony, all cursors may be held over commit.
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

//#ENDIF

//#IFDEF(JDK1.6)

    // -------------------------- JDK 1.6 -----------------------------------

    public boolean isClosed() throws SQLException {
        return result_set_list == null;
    }

    public void setPoolable(boolean poolable) throws SQLException {
    }

    public boolean isPoolable() throws SQLException {
        return true;
    }

    public Object unwrap(Class iface) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public boolean isWrapperFor(Class iface) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw MSQLException.unsupported16();
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw MSQLException.unsupported16();
    }

//#ENDIF

//#IFDEF(JDBC5.0)

    // -------------------------- JDK 1.7 -----------------------------------

    public void closeOnCompletion() throws SQLException {
        // This is a no-op in Pony.
        // The reason being that no resources are consumed when all the result
        // sets created by a statement are closed. Therefore it's safe to have
        // this be a 'no-op'.
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

//#ENDIF


    // ---------- Finalize ----------

    /**
     * The statement will close when it is garbage collected.
     */
    public void finalize() {
        try {
            close();
        } catch (SQLException e) { /* ignore */ }
    }

}
