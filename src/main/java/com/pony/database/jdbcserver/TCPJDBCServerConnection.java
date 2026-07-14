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

package com.pony.database.jdbcserver;

import com.pony.database.jdbc.DatabaseInterface;
import com.pony.debug.DebugLogger;

import java.net.Socket;
import java.io.*;

/**
 * A ServerConnection that processes JDBC queries from a client from a
 * TCP Socket.
 *
 * @author Tobias Downer
 */

final class TCPJDBCServerConnection extends StreamJDBCServerConnection {

    /**
     * The socket connection with the client.
     */
    private Socket connection;

    /**
     * Is set to true when the connection to the client is closed.
     */
    private boolean is_closed = false;

    /**
     * Constructs the ServerConnection object.
     */
    TCPJDBCServerConnection(DatabaseInterface db_interface,
                            Socket socket, DebugLogger logger) throws IOException {
        super(db_interface, socket.getInputStream(),
                socket.getOutputStream(), logger);
        this.connection = socket;
    }

    /**
     * Completely closes the connection to the client.
     */
    public void close() throws IOException {
        try {
            // Dispose the processor
            dispose();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        // Close the socket
        connection.close();
        is_closed = true;
    }

    /**
     * Returns true if the connection to the client has been closed.
     */
    public boolean isClosed() throws IOException {
        return is_closed;
    }

}
