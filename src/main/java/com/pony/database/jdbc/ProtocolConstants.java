/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
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
