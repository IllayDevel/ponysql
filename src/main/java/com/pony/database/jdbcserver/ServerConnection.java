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

import java.io.IOException;

/**
 * A server side connection with a client.  Each client that is connected
 * to the database has a ServerConnection object.
 *
 * @author Tobias Downer
 */

interface ServerConnection {

    /**
     * This should return true if it has been determined that there is an
     * entire command waiting to be serviced on this connection.  This method
     * is always run on the same thread for all connections.  It is called
     * many times a second by the connection pool server so it must execute
     * extremely fast.
     * <p>
     * ISSUE: Method is polled!  Unfortunately can't get around this because
     *   of the limitation in Java that TCP connections must block on a thread,
     *   and we can't block if we are to be servicing 100+ connections.
     */
    boolean requestPending() throws IOException;

    /**
     * Processes a pending command on the connection.  This method is called
     * from a database worker thread.  The method will block until a request
     * has been received and processed.  Note, it is not desirable is some
     * cases to allow this method to block.  If a call to 'requestPending'
     * returns true then then method is guarenteed not to block.
     * <p>
     * The first call to this method will handle the hand shaking protocol
     * between the client and server.
     * <p>
     * While this method is doing something, it can not be called again even
     * if another request arrives from the client.  All calls to this method
     * are sequential.  This method will only be called if the 'ping' method is
     * not currently being processed.
     */
    void processRequest() throws IOException;

    /**
     * Blocks until a complete command is available to be processed.  This is
     * used for a blocking implementation.  As soon as this method returns then
     * a call to 'processRequest' will process the incoming command.
     */
    void blockForRequest() throws IOException;

    /**
     * Pings the connection.  This is used to determine if the connection is
     * alive or not.  If it's not, we should throw an IOException.
     * <p>
     * This method will only be called if the 'processRequest' method is not
     * being processed.
     */
    void ping() throws IOException;

    /**
     * Closes this connection.
     */
    void close() throws IOException;


}
