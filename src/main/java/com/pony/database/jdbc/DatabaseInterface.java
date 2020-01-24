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

package com.pony.database.jdbc;

import java.sql.SQLException;

/**
 * The interface with the Database whether it be remotely via TCP/IP or
 * locally within the current JVM.
 *
 * @author Tobias Downer
 */

public interface DatabaseInterface {

    /**
     * Attempts to log in to the database as the given username with the given
     * password.  Only one user may be authenticated per connection.  This must
     * be called before the other methods are used.
     * <p>
     * A DatabaseCallBack implementation must be given here that is notified
     * of all events from the database.  Events are only received if the
     * login was successful.
     */
    boolean login(String default_schema, String username, String password,
                  DatabaseCallBack call_back) throws SQLException;

    /**
     * Pushes a part of a streamable object from the client onto the server.  The
     * server stores the large object for use with a future query.  For example,
     * a sequence of with a query with large objects may operate as follows;
     * <p><pre>
     * 1) Push 100 MB object (id = 104)
     * 2) execQuery with query that contains a streamable object with id 104
     * </pre><p>
     * Note that the client may push any part of a streamable object onto the
     * server, however the streamable object must have been completely pushed
     * for the query to execute correctly.  For example, an 100 MB byte array may
     * be pushed onto the server in blocks of 64K (in 1,600 separate blocks).
     * <p>
     * @param type the StreamableObject type (1 = byte array, 2 = char array)
     * @param object_id the identifier of the StreamableObject for future queries.
     * @param object_length the total length of the StreamableObject.
     * @param buf the byte[] array representing the block of information being
     *   sent.
     * @param offset the offset into of the object of this block.
     * @param length the length of the block being pushed.
     */
    void pushStreamableObjectPart(byte type, long object_id, long object_length,
                                  byte[] buf, long offset, int length) throws SQLException;

    /**
     * Executes the query and returns a QueryResponse object that describes the
     * result of the query.  The QueryResponse object describes the number of
     * rows, describes the columns, etc.  This method will block until the query
     * has completed.  The QueryResponse can be used to obtain the 'result id'
     * variable that is used in subsequent queries to the engine to retrieve
     * the actual result of the query.
     */
    QueryResponse execQuery(SQLQuery sql) throws SQLException;

    /**
     * Returns a part of a result set.  The result set part is referenced via the
     * 'result id' found in the QueryResponse.  This is used to read parts
     * of the query once it has been found via 'execQuery'.
     * <p>
     * The returned List object contains the result requested.
     * <p>
     * If the result contains any StreamableObject objects, then the server
     * allocates a channel to the object via the 'getStreamableObjectPart' and
     * the identifier of the StreamableObject.  The channel may only be disposed
     * if the 'disposeStreamableObject' method is called.
     */
    ResultPart getResultPart(int result_id, int row_number, int row_count)
            throws SQLException;

    /**
     * Disposes of a result of a query on the server.  This frees up server side
     * resources allocated to a query.  This should be called when the ResultSet
     * of a query closes.  We should try and use this method as soon as possible
     * because it frees locks on tables and allows deleted rows to be
     * reclaimed.
     */
    void disposeResult(int result_id) throws SQLException;

    /**
     * Returns a section of a large binary or character stream in a result set.
     * This is used to stream large values over the connection.  For example, if
     * a row contained a multi megabyte object and the client is only interested
     * in the first few characters and the last few characters of the stream.
     * This would require only a few queries to the database and the multi-
     * megabyte object would not need to be downloaded to the client in its
     * entirety.
     */
    StreamableObjectPart getStreamableObjectPart(int result_id,
                                                 long streamable_object_id, long offset, int len) throws SQLException;

    /**
     * Disposes a streamable object channel with the given identifier.  This
     * should be called to free any resources on the server associated with the
     * object.  It should be called as soon as possible because it frees locks on
     * the tables and allows deleted rows to be reclaimed.
     */
    void disposeStreamableObject(int result_id, long streamable_object_id)
            throws SQLException;

    /**
     * Called when the connection is disposed.  This will terminate the
     * connection if there is any connection to terminate.
     */
    void dispose() throws SQLException;

}
