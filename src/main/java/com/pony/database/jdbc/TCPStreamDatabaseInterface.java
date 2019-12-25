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
import java.net.*;

/**
 * Connection to the database via the TCP protocol.
 *
 * @author Tobias Downer
 */

class TCPStreamDatabaseInterface extends StreamDatabaseInterface {

    /**
     * The name of the host we are connected to.
     */
    private final String host;

    /**
     * The port we are connected to.
     */
    private final int port;

    /**
     * The Socket connection.
     */
    private Socket socket;

    /**
     * Constructor.
     */
    TCPStreamDatabaseInterface(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connects to the database.
     */
    void connectToDatabase() throws SQLException {
        if (socket != null) {
            throw new SQLException("Connection already established.");
        }
        try {
            // Open a socket connection to the server.
            socket = new Socket(host, port);
            // Setup the stream with the given input and output streams.
            setup(socket.getInputStream(), socket.getOutputStream());
        } catch (IOException e) {
            throw new SQLException(e.getMessage());
        }
    }

}
