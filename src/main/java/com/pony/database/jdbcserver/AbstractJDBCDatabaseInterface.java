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

package com.pony.database.jdbcserver;

import com.pony.database.*;
import com.pony.database.global.*;
import com.pony.database.interpret.Statement;
import com.pony.database.interpret.SQLQueryExecutor;
import com.pony.database.sql.SQL;
import com.pony.database.sql.ParseException;
import com.pony.database.jdbc.*;
import com.pony.util.IntegerVector;
import com.pony.util.StringUtil;
import com.pony.debug.*;

import java.sql.SQLException;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

/**
 * An abstract implementation of JDBCDatabaseInterface that provides a
 * connection between a single DatabaseConnection and a DatabaseInterface
 * implementation.
 * <p>
 * This receives database commands from the JDBC layer and dispatches the
 * queries to the database system.  It also manages ResultSet maps for query
 * results.
 * <p>
 * This implementation does not handle authentication (login) / construction
 * of the DatabaseConnection object, or disposing of the connection.
 * <p>
 * This implementation ignores the AUTO-COMMIT flag when a query is executed.
 * To implement AUTO-COMMIT, you should 'commit' after a command is executed.
 * <p>
 * SYNCHRONIZATION: This interface is NOT thread-safe.  To make a thread-safe
 *   implementation use the LockingMechanism.
 * <p>
 * See JDBCDatabaseInterface for a standard server-side implementation of this
 * class.
 *
 * @author Tobias Downer
 */

public abstract class AbstractJDBCDatabaseInterface
        implements DatabaseInterface {

    /**
     * The Databas object that represents the context of this
     * database interface.
     */
    private Database database;

    /**
     * The mapping that maps from result id number to Table object that this
     * JDBC connection is currently maintaining.
     * <p>
     * NOTE: All Table objects are now valid over a database shutdown + init.
     */
    private HashMap result_set_map;

    /**
     * This is incremented every time a result set is added to the map.  This
     * way, we always have a unique key on hand.
     */
    private int unique_result_id;

    /**
     * Access to information regarding the user logged in on this connection.
     * If no user is logged in, this is left as 'null'.  We can also use this to
     * retreive the Database object the user is logged into.
     */
    private User user = null;

    /**
     * The database connection transaction.
     */
    private DatabaseConnection database_connection;

    /**
     * The SQL parser object for this interface.  When a statement is being
     * parsed, this object is sychronized.
     */
    private SQLQueryExecutor sql_executor;
//  private SQL sql_parser;

    /**
     * Mantains a mapping from streamable object id for a particular object that
     * is currently being uploaded to the server.  This maps streamable_object_id
     * to blob id reference.
     */
    private HashMap blob_id_map;

    /**
     * Set to true when this database interface is disposed.
     */
    private boolean disposed;


    /**
     * Sets up the database interface.
     */
    public AbstractJDBCDatabaseInterface(Database database) {
        this.database = database;
        result_set_map = new HashMap();
        blob_id_map = new HashMap();
        unique_result_id = 1;
        disposed = false;
    }

    // ---------- Utility methods ----------

    /**
     * Initializes this database interface with a User and DatabaseConnection
     * object.  This would typically be called from inside an authentication
     * method, or from 'login'.  This must be set before the object can be
     * used.
     */
    protected final void init(User user, DatabaseConnection connection) {
        this.user = user;
        this.database_connection = connection;
        // Set up the sql parser.
        sql_executor = new SQLQueryExecutor();
//    sql_parser = new SQL(new StringReader(""));
    }

    /**
     * Returns the Database that is the context of this interface.
     */
    protected final Database getDatabase() {
        return database;
    }

    /**
     * Returns the User object for this connection.
     */
    protected final User getUser() {
        return user;
    }

    /**
     * Returns a DebugLogger object that can be used to log debug messages
     * against.
     */
    public final DebugLogger Debug() {
        return getDatabase().Debug();
    }

    /**
     * Returns the DatabaseConnection objcet for this connection.
     */
    protected final DatabaseConnection getDatabaseConnection() {
        return database_connection;
    }

    /**
     * Adds this result set to the list of result sets being handled through
     * this processor.  Returns a number that unique identifies this result
     * set.
     */
    private int addResultSet(ResultSetInfo result) {
        // Lock the roots of the result set.
        result.lockRoot(-1);  // -1 because lock_key not implemented

        // Make a new result id
        int result_id;
        // This ensures this block can handle concurrent updates.
        synchronized (result_set_map) {
            result_id = ++unique_result_id;
            // Add the result to the map.
            result_set_map.put(new Integer(result_id), result);
        }

        return result_id;
    }

    /**
     * Gets the result set with the given result_id.
     */
    private ResultSetInfo getResultSet(int result_id) {
        synchronized (result_set_map) {
            return (ResultSetInfo) result_set_map.get(new Integer(result_id));
        }
    }

    /**
     * Disposes of the result set with the given result_id.  After this has
     * been called, the GC should garbage the table.
     */
    private void disposeResultSet(int result_id) {
        // Remove this entry.
        ResultSetInfo table;
        synchronized (result_set_map) {
            table = (ResultSetInfo) result_set_map.remove(new Integer(result_id));
        }
        if (table != null) {
            table.dispose();
        } else {
            Debug().write(Lvl.ERROR, this,
                    "Attempt to dispose invalid 'result_id'.");
        }
    }

    /**
     * Clears the contents of the result set map.  This removes all result_id
     * ResultSetInfo maps.
     */
    protected final void clearResultSetMap() {
        Iterator keys;
        ArrayList list;
        synchronized (result_set_map) {
            keys = result_set_map.keySet().iterator();

            list = new ArrayList();
            while (keys.hasNext()) {
                list.add(keys.next());
            }
        }
        keys = list.iterator();

        while (keys.hasNext()) {
            int result_id = ((Integer) keys.next()).intValue();
            disposeResultSet(result_id);
        }
    }

    /**
     * Wraps a Throwable thrown by the execution of a query in DatabaseConnection
     * with an SQLException and puts the appropriate error messages to the debug
     * log.
     */
    protected final SQLException handleExecuteThrowable(Throwable e,
                                                        SQLQuery query) {
        if (e instanceof ParseException) {

            Debug().writeException(Lvl.WARNING, e);

            // Parse exception when parsing the SQL.
            String msg = e.getMessage();
            msg = StringUtil.searchAndReplace(msg, "\r", "");
            return new MSQLException(msg, msg, 35, e);

        } else if (e instanceof TransactionException) {

            TransactionException te = (TransactionException) e;

            // Output query that was in error to debug log.
            Debug().write(Lvl.INFORMATION, this,
                    "Transaction error on: " + query);
            Debug().writeException(Lvl.INFORMATION, e);

            // Denotes a transaction exception.
            return new MSQLException(e.getMessage(), e.getMessage(),
                    200 + te.getType(), e);
        } else {

            // Output query that was in error to debug log.
            Debug().write(Lvl.WARNING, this,
                    "Exception thrown during query processing on: " + query);
            Debug().writeException(Lvl.WARNING, e);

            // Error, we need to return exception to client.
            return new MSQLException(e.getMessage(), e.getMessage(), 1, e);

        }

    }

    /**
     * Returns a reference implementation object that handles an object that is
     * either currently being pushed onto the server from the client, or is being
     * used to reference a large object in an SQLQuery.
     */
    private Ref getLargeObjectRefFor(long streamable_object_id, byte type,
                                     long object_length) {
        // Does this mapping already exist?
        Long s_ob_id = new Long(streamable_object_id);
        Object ob = blob_id_map.get(s_ob_id);
        if (ob == null) {
            // Doesn't exist so create a new blob handler.
            Ref ref = database_connection.createNewLargeObject(type, object_length);
            // Make the blob id mapping
            blob_id_map.put(s_ob_id, ref);
            // And return it
            return ref;
        } else {
            // Exists so use this blob ref.
            return (Ref) ob;
        }
    }

    /**
     * Returns a reference object that handles the given streamable object id
     * in this database interface.  Unlike the other 'getLargeObjectRefFor
     * method, this will not create a new handle if it has not already been
     * formed before by this connection.  If the large object ref is not found
     * an exception is generated.
     */
    private Ref getLargeObjectRefFor(long streamable_object_id)
            throws SQLException {
        Long s_ob_id = new Long(streamable_object_id);
        Object ob = blob_id_map.get(s_ob_id);
        if (ob == null) {
            // This basically means the streamable object hasn't been pushed onto the
            // server.
            throw new SQLException("Invalid streamable object id in query.");
        } else {
            return (Ref) ob;
        }
    }

    /**
     * Removes the large object reference from the HashMap for the given
     * streamable object id from the HashMap.  This allows the Ref to finalize if
     * the VM does not maintain any other pointers to it, and therefore clean up
     * the resources in the store.
     */
    private Ref flushLargeObjectRefFromCache(long streamable_object_id)
            throws SQLException {
        try {
            Long s_ob_id = new Long(streamable_object_id);
            Object ob = blob_id_map.remove(s_ob_id);
            if (ob == null) {
                // This basically means the streamable object hasn't been pushed onto the
                // server.
                throw new SQLException("Invalid streamable object id in query.");
            } else {
                Ref ref = (Ref) ob;
                // Mark the blob as complete
                ref.complete();
                // And return it.
                return ref;
            }
        } catch (IOException e) {
            Debug().writeException(e);
            throw new SQLException("IO Error: " + e.getMessage());
        }
    }

    /**
     * Disposes all resources associated with this object.  This clears the
     * ResultSet map, and NULLs all references to help the garbage collector.
     * This method would normally be called from implementations of the
     * 'dispose' method.
     */
    protected final void internalDispose() {
        disposed = true;
        // Clear the result set mapping
        clearResultSetMap();
        user = null;
        database_connection = null;
        sql_executor = null;
    }

    /**
     * Checks if the interface is disposed, and if it is generates a friendly
     * SQLException informing the user of this.
     */
    protected final void checkNotDisposed() throws SQLException {
        if (disposed) {
            throw new SQLException(
                    "Database interface was disposed (was the connection closed?)");
        }
    }

    // ---------- Implemented from DatabaseInterface ----------


    public void pushStreamableObjectPart(byte type, long object_id,
                                         long object_length, byte[] buf, long offset, int length)
            throws SQLException {
        checkNotDisposed();

        try {
            // Create or retrieve the object managing this binary object_id in this
            // connection.
            Ref ref = getLargeObjectRefFor(object_id, type, object_length);
            // Push this part of the blob into the object.
            ref.write(offset, buf, length);
        } catch (IOException e) {
            Debug().writeException(e);
            throw new SQLException("IO Error: " + e.getMessage());
        }

    }


    public QueryResponse execQuery(SQLQuery query) throws SQLException {

        checkNotDisposed();

        // Record the query start time
        long start_time = System.currentTimeMillis();
        // Where query result eventually resides.
        ResultSetInfo result_set_info;
        int result_id = -1;

        // For each StreamableObject in the SQLQuery object, translate it to a
        // Ref object that presumably has been pre-pushed onto the server from
        // the client.
        boolean blobs_were_flushed = false;
        Object[] vars = query.getVars();
        if (vars != null) {
            for (int i = 0; i < vars.length; ++i) {
                Object ob = vars[i];
                // This is a streamable object, so convert it to a *Ref
                if (ob != null && ob instanceof StreamableObject) {
                    StreamableObject s_object = (StreamableObject) ob;
                    // Flush the streamable object from the cache
                    // Note that this also marks the blob as complete in the blob store.
                    Ref ref = flushLargeObjectRefFromCache(s_object.getIdentifier());
                    // Set the Ref object in the query.
                    vars[i] = ref;
                    // There are blobs in this query that were written to the blob store.
                    blobs_were_flushed = true;
                }
            }
        }

        // After the blobs have been flushed, we must tell the connection to
        // flush and synchronize any blobs that have been written to disk.  This
        // is an important (if subtle) step.
        if (blobs_were_flushed) {
            database_connection.flushBlobStore();
        }

        try {

            // Evaluate the sql query.
            Table result = sql_executor.execute(database_connection, query);

            // Put the result in the result cache...  This will lock this object
            // until it is removed from the result set cache.  Returns an id that
            // uniquely identifies this result set in future communication.
            // NOTE: This locks the roots of the table so that its contents
            //   may not be altered.
            result_set_info = new ResultSetInfo(query, result);
            result_id = addResultSet(result_set_info);

        } catch (Throwable e) {
            // If result_id set, then dispose the result set.
            if (result_id != -1) {
                disposeResultSet(result_id);
            }

            // Handle the throwable during query execution
            throw handleExecuteThrowable(e, query);

        }

        // The time it took the query to execute.
        long taken = System.currentTimeMillis() - start_time;

        // Return the query response
        return new JDIQueryResponse(result_id, result_set_info, (int) taken, "");

    }


    public ResultPart getResultPart(int result_id, int row_number,
                                    int row_count) throws SQLException {

        checkNotDisposed();

        ResultSetInfo table = getResultSet(result_id);
        if (table == null) {
            throw new MSQLException("'result_id' invalid.", null, 4,
                    (Throwable) null);
        }

        int row_end = row_number + row_count;

        if (row_number < 0 || row_number >= table.getRowCount() ||
                row_end > table.getRowCount()) {
            throw new MSQLException("Result part out of range.", null, 4,
                    (Throwable) null);
        }

        try {
            int col_count = table.getColumnCount();
            ResultPart block = new ResultPart(row_count * col_count);
            for (int r = row_number; r < row_end; ++r) {
                for (int c = 0; c < col_count; ++c) {
                    TObject t_object = table.getCellContents(c, r);
                    // If this is a Ref, we must assign it a streamable object
                    // id that the client can use to access the large object.
                    Object client_ob;
                    if (t_object.getObject() instanceof Ref) {
                        Ref ref = (Ref) t_object.getObject();
                        client_ob = new StreamableObject(ref.getType(),
                                ref.getRawSize(), ref.getID());
                    } else {
                        client_ob = t_object.getObject();
                    }
                    block.addElement(client_ob);
                }
            }
            return block;
        } catch (Throwable e) {
            Debug().writeException(Lvl.WARNING, e);
            // If an exception was generated while getting the cell contents, then
            // throw an SQLException.
            throw new MSQLException(
                    "Exception while reading results: " + e.getMessage(),
                    e.getMessage(), 4, e);
        }

    }


    public void disposeResult(int result_id) throws SQLException {
        // Check the DatabaseInterface is not dispoed
        checkNotDisposed();
        // Dispose the result
        disposeResultSet(result_id);
    }


    public StreamableObjectPart getStreamableObjectPart(int result_id,
                                                        long streamable_object_id, long offset, int len) throws SQLException {

        checkNotDisposed();

        // NOTE: It's important we handle the 'result_id' here and don't just
        //   treat the 'streamable_object_id' as a direct reference into the
        //   blob store.  If we don't authenticate a streamable object against its
        //   originating result, we can't guarantee the user has permission to
        //   access the data.  This would mean a malicious client could access
        //   BLOB data they may not be permitted to look at.
        //   This also protects us from clients that might send a bogus
        //   streamable_object_id and cause unpredictible results.

        ResultSetInfo table = getResultSet(result_id);
        if (table == null) {
            throw new MSQLException("'result_id' invalid.", null, 4,
                    (Throwable) null);
        }

        // Get the large object ref that has been cached in the result set.
        Ref ref = table.getRef(streamable_object_id);
        if (ref == null) {
            throw new MSQLException("'streamable_object_id' invalid.", null, 4,
                    (Throwable) null);
        }

        // Restrict the server so that a streamable object part can not exceed
        // 512 KB.
        if (len > 512 * 1024) {
            throw new MSQLException("Request length exceeds 512 KB", null, 4,
                    (Throwable) null);
        }

        try {
            // Read the blob part into the byte array.
            byte[] blob_part = new byte[len];
            ref.read(offset, blob_part, len);

            // And return as a StreamableObjectPart object.
            return new StreamableObjectPart(blob_part);

        } catch (IOException e) {
            throw new MSQLException(
                    "Exception while reading blob: " + e.getMessage(),
                    e.getMessage(), 4, e);
        }

    }


    public void disposeStreamableObject(int result_id, long streamable_object_id)
            throws SQLException {
        checkNotDisposed();

        // This actually isn't as an important step as I had originally designed
        // for.  To dispose we simply remove the blob ref from the cache in the
        // result.  If this doesn't happen, nothing seriously bad will happen.

        ResultSetInfo table = getResultSet(result_id);
        if (table == null) {
            throw new MSQLException("'result_id' invalid.", null, 4,
                    (Throwable) null);
        }

        // Remove this Ref from the table
        table.removeRef(streamable_object_id);

    }


    // ---------- Clean up ----------

    /**
     * Clean up if this object is GC'd.
     */
    public void finalize() throws Throwable {
        super.finalize();
        try {
            if (!disposed) {
                dispose();
            }
        } catch (Throwable e) { /* ignore this */ }
    }

    // ---------- Inner classes ----------

    /**
     * The response to a query.
     */
    private final static class JDIQueryResponse implements QueryResponse {

        int result_id;
        ResultSetInfo result_set_info;
        int query_time;
        String warnings;

        JDIQueryResponse(int result_id, ResultSetInfo result_set_info,
                         int query_time, String warnings) {
            this.result_id = result_id;
            this.result_set_info = result_set_info;
            this.query_time = query_time;
            this.warnings = warnings;
        }

        public int getResultID() {
            return result_id;
        }

        public int getQueryTimeMillis() {
            return query_time;
        }

        public int getRowCount() {
            return result_set_info.getRowCount();
        }

        public int getColumnCount() {
            return result_set_info.getColumnCount();
        }

        public ColumnDescription getColumnDescription(int n) {
            return result_set_info.getFields()[n];
        }

        public String getWarnings() {
            return warnings;
        }

    }


    /**
     * Whenever a ResultSet is generated, this object contains the result set.
     * This class only allows calls to safe methods in Table.
     * <p>
     * NOTE: This is safe provided,
     *   a) The column topology doesn't change (NOTE: issues with ALTER command)
     *   b) Root locking prevents modification to rows.
     */
    private final static class ResultSetInfo {

        /**
         * The SQLQuery that was executed to produce this result.
         */
        private SQLQuery query;

        /**
         * The table that is the result.
         */
        private Table result;

        /**
         * A set of ColumnDescription that describes each column in the ResultSet.
         */
        private ColumnDescription[] col_desc;

        /**
         * IntegerVector that contains the row index into the table for each
         * row of the result.  For example, row.intAt(5) will return the row index
         * of 'result' of the 5th row item.
         */
        private IntegerVector row_index_map;

        /**
         * Set to true if the result table has a SimpleRowEnumeration, therefore
         * guarenteeing we do not need to store a row lookup list.
         */
        private boolean result_is_simple_enum;

        /**
         * The number of rows in the result.
         */
        private int result_row_count;

        /**
         * Incremented when we lock roots.
         */
        private int locked;

        /**
         * A HashMap of blob_reference_id values to Ref objects used to handle
         * and streamable objects in this result.
         */
        private HashMap streamable_blob_map;


        /**
         * Constructs the result set.
         */
        ResultSetInfo(SQLQuery query, Table table) {
            this.query = query;
            this.result = table;
            this.streamable_blob_map = new HashMap();

            result_row_count = table.getRowCount();

            // HACK: Read the contents of the first row so that we can pick up
            //   any errors with reading, and also to fix the 'uniquekey' bug
            //   that causes a new transaction to be started if 'uniquekey' is
            //   a column and the value is resolved later.
            RowEnumeration row_enum = table.rowEnumeration();
            if (row_enum.hasMoreRows()) {
                int row_index = row_enum.nextRowIndex();
                for (int c = 0; c < table.getColumnCount(); ++c) {
                    table.getCellContents(c, row_index);
                }
            }
            // If simple enum, note it here
            result_is_simple_enum = (row_enum instanceof SimpleRowEnumeration);
            row_enum = null;

            // Build 'row_index_map' if not a simple enum
            if (!result_is_simple_enum) {
                row_index_map = new IntegerVector(table.getRowCount());
                RowEnumeration en = table.rowEnumeration();
                while (en.hasMoreRows()) {
                    row_index_map.addInt(en.nextRowIndex());
                }
            }

            // This is a safe operation provides we are shared.
            // Copy all the TableField columns from the table to our own
            // ColumnDescription array, naming each column by what is returned from
            // the 'getResolvedVariable' method.
            final int col_count = table.getColumnCount();
            col_desc = new ColumnDescription[col_count];
            for (int i = 0; i < col_count; ++i) {
                Variable v = table.getResolvedVariable(i);
                String field_name;
                if (v.getTableName() == null) {
                    // This means the column is an alias
                    field_name = "@a" + v.getName();
                } else {
                    // This means the column is an schema/table/column reference
                    field_name = "@f" + v.toString();
                }
                col_desc[i] =
                        table.getColumnDefAt(i).columnDescriptionValue(field_name);
//        col_desc[i] = new ColumnDescription(field_name, table.getFieldAt(i));
            }

            locked = 0;
        }

        /**
         * Returns the SQLQuery that was used to produce this result.
         */
        SQLQuery getSQLQuery() {
            return query;
        }

        /**
         * Returns a Ref that has been cached in this table object by its
         * identifier value.
         */
        Ref getRef(long id) {
            return (Ref) streamable_blob_map.get(new Long(id));
        }

        /**
         * Removes a Ref that has been cached in this table object by its
         * identifier value.
         */
        void removeRef(long id) {
            streamable_blob_map.remove(new Long(id));
        }

        /**
         * Disposes this object.
         */
        void dispose() {
            while (locked > 0) {
                unlockRoot(-1);
            }
            result = null;
            row_index_map = null;
            col_desc = null;
        }

        /**
         * Gets the cell contents of the cell at the given row/column.
         * <p>
         * Safe only if roots are locked.
         */
        TObject getCellContents(int column, int row) {
            if (locked > 0) {
                int real_row;
                if (result_is_simple_enum) {
                    real_row = row;
                } else {
                    real_row = row_index_map.intAt(row);
                }
                TObject tob = result.getCellContents(column, real_row);

                // If this is a large object reference then cache it so a streamable
                // object can reference it via this result.
                if (tob.getObject() instanceof Ref) {
                    Ref ref = (Ref) tob.getObject();
                    streamable_blob_map.put(new Long(ref.getID()), ref);
                }

                return tob;
            } else {
                throw new RuntimeException("Table roots not locked!");
            }
        }

        /**
         * Returns the column count.
         */
        int getColumnCount() {
            return result.getColumnCount();
        }

        /**
         * Returns the row count.
         */
        int getRowCount() {
            return result_row_count;
        }

        /**
         * Returns the ColumnDescription array of all the columns in the result.
         */
        ColumnDescription[] getFields() {
            return col_desc;
        }

        /**
         * Locks the root of the result set.
         */
        void lockRoot(int key) {
            result.lockRoot(key);
            ++locked;
        }

        /**
         * Unlocks the root of the result set.
         */
        void unlockRoot(int key) {
            result.unlockRoot(key);
            --locked;
        }

    }

}
