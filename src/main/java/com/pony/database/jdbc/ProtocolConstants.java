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

/**
 * Constants used in the JDBC database communication protocol.
 *
 * @author Tobias Downer
 */

public interface ProtocolConstants {

    /**
     * Sent as an acknowledgement to a command.
     */
    int ACKNOWLEDGEMENT = 5;

    /**
     * Sent if login passed.
     */
    int USER_AUTHENTICATION_PASSED = 10;

    /**
     * Sent if login failed because username or password were invalid.
     */
    int USER_AUTHENTICATION_FAILED = 15;

    /**
     * Operation was successful.
     */
    int SUCCESS = 20;

    /**
     * Operation threw an exception.
     */
    int EXCEPTION = 30;

    /**
     * There was an authentication error.  A query couldn't be executed because
     * the user does not have enough rights.
     */
    int AUTHENTICATION_ERROR = 35;


    // ---------- Commands ----------

    /**
     * Query sent to the server for processing.
     */
    int QUERY = 50;

    /**
     * Disposes the server-side resources associated with a result.
     */
    int DISPOSE_RESULT = 55;

    /**
     * Requests a section of a result from the server.
     */
    int RESULT_SECTION = 60;

    /**
     * Requests a section of a streamable object from the server.
     */
    int STREAMABLE_OBJECT_SECTION = 61;

    /**
     * Disposes of the resources associated with a streamable object on the
     * server.
     */
    int DISPOSE_STREAMABLE_OBJECT = 62;

    /**
     * For pushing a part of a streamable object onto the server from the client.
     */
    int PUSH_STREAMABLE_OBJECT_PART = 63;


    /**
     * Ping command.
     */
    int PING = 65;

    /**
     * Closes the protocol stream.
     */
    int CLOSE = 70;

    /**
     * Denotes an event from the database (trigger, etc).
     */
    int DATABASE_EVENT = 75;

    /**
     * Denotes a server side request for information.  For example, a request for
     * a part of a streamable object.
     */
    int SERVER_REQUEST = 80;


}
