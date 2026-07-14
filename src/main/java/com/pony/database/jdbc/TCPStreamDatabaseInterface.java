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
