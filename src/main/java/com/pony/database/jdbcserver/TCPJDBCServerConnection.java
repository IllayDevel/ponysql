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
