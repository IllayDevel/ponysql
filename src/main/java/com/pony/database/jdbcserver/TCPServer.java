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

import com.pony.database.DatabaseSystem;
import com.pony.database.Database;
import com.pony.debug.*;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.io.IOException;
import java.util.HashMap;
import java.util.ResourceBundle;

/**
 * A TCP/IP socket server that opens a single port and allows JDBC clients
 * to connect through the port to talk with the database.
 *
 * @author Tobias Downer
 */

public final class TCPServer {

    /**
     * The parent Database object that describes everything about the
     * database this TCP server is for.
     */
    private final Database database;

    /**
     * The ConnectionPoolServer that polls the ServerConnection for new commands
     * to process.
     */
    private ConnectionPoolServer connection_pool;

    /**
     * The ServerSocket object where the database server is bound.
     */
    private ServerSocket server_socket;

    /**
     * The InetAddress the JDBC server is bound to.
     */
    private InetAddress address;

    /**
     * The port the JDBC server is on.
     */
    private int port;

    /**
     * The connection pool model used for this server.
     */
    private String connection_pool_model;

    /**
     * Constructs the TCPServer over the given DatabaseSystem configuration.
     */
    public TCPServer(Database database) {
        this.database = database;
    }

    /**
     * Returns a DebugLogger object that we can log debug messages to.
     */
    public final DebugLogger Debug() {
        return database.Debug();
    }

    /**
     * Returns the port the JDBC server is on.
     */
    public int getJDBCPort() {
        return port;
    }

    /**
     * Checks to see if there's already something listening on the jdbc
     * port.  Returns true if the jdbc port in the configuration is available,
     * otherwise returns false.
     */
    public boolean checkAvailable(InetAddress bind_address, int tcp_port) {
        // MAJOR MAJOR HACK: We attempt to bind to the JDBC Port and if we get
        //   an error then most likely there's already a database running on this
        //   host.

        int port = tcp_port;
//    // Get information about how to set up the TCP port from the config
//    // bundle.
//    int port = Integer.parseInt(config.getString("jdbc_server_port"));

        try {
            // Bind the ServerSocket objects to the ports.
            server_socket = new ServerSocket(port, 50, bind_address);
            server_socket.close();
        } catch (IOException e) {
            // If error then return false.
            return false;
        }

        return true;
    }


    /**
     * Starts the server running.  This method returns immediately but spawns
     * its own thread.
     */
    public void start(InetAddress bind_address, int tcp_port,
                      String connection_pool_model) {

        this.address = bind_address;
        this.port = tcp_port;
        this.connection_pool_model = connection_pool_model;

//    // Get information about how to set up the TCP port from the config
//    // bundle.
//    port = Integer.parseInt(config.getString("jdbc_server_port"));
//
//    // The 'tcp_connection_pool_thread_model' property determines the
//    // connection pool object to use.
//    connection_pool_model = "multi_threaded";
//    try {
//      String cptm = config.getString("tcp_connection_pool_thread_model");
//      if (cptm.equalsIgnoreCase("single_threaded")) {
//        connection_pool_model = "single_threaded";
//      }
//      // Multi-threaded if 'tcp_connection_pool_thread_model' is anything
//      // other than 'single_threaded'
//    }
//    catch (java.util.MissingResourceException e) {
//      // If no property in the config assume multi-threaded
//    }
        // Choose our connection pool implementation
        if (connection_pool_model.equals("multi_threaded")) {
            this.connection_pool = new MultiThreadedConnectionPoolServer(database);
        } else if (connection_pool_model.equals("single_threaded")) {
            this.connection_pool = new SingleThreadedConnectionPoolServer(database);
        }

        try {
            // Bind the ServerSocket object to the port.
            server_socket = new ServerSocket(port, 50, bind_address);
            server_socket.setSoTimeout(0);
        } catch (IOException e) {
            Debug().writeException(e);
            Debug().write(Lvl.ERROR, this,
                    "Unable to start a server socket on port: " + port);
            throw new Error(e.getMessage());
        }

        // This thread blocks on the server socket.
        Thread listen_thread = new Thread() {
            public void run() {
                try {
                    // Accept new connections and notify when one arrives
                    while (true) {
                        Socket s = server_socket.accept();
                        portConnection(s);
                    }
                } catch (IOException e) {
                    Debug().writeException(Lvl.WARNING, e);
                    Debug().write(Lvl.WARNING, this, "Socket listen thread died.");
                }
            }
        };

//    listen_thread.setDaemon(true);
        listen_thread.setName("Pony - TCP/IP Socket Accept");

        listen_thread.start();

    }

    /**
     * Called whenever a new connection has been received on the port.
     */
    private void portConnection(Socket socket) throws IOException {
        // TCP connections are formatted as;
        // 'TCP/[ip address]:[remote port]:[local port]'
        String host_string = "TCP/" + socket.getInetAddress().getHostAddress() +
                ":" + socket.getPort() + "@" +
                socket.getLocalAddress().getHostAddress() + ":" +
                socket.getLocalPort();
//    String host_string =
//      "Host [" + socket.getInetAddress().getHostAddress() + "] " +
//      "port=" + socket.getPort() + " localport=" + socket.getLocalPort();
        // Make a new DatabaseInterface for this connection,
        JDBCDatabaseInterface db_interface =
                new JDBCDatabaseInterface(database, host_string);
        TCPJDBCServerConnection connection =
                new TCPJDBCServerConnection(db_interface, socket, Debug());
        // Add the provider onto the queue of providers that are serviced by
        // the server.
        connection_pool.addConnection(connection);
    }

    /**
     * Closes the JDBC Server.
     */
    public void close() {
        if (server_socket != null) {
            try {
                server_socket.close();
            } catch (IOException e) {
                Debug().write(Lvl.ERROR, this, "Error closing JDBC Server.");
                Debug().writeException(e);
            }
        }
        connection_pool.close();
    }

    /**
     * Returns human understandable information about the server.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("TCP JDBC Server (");
        buf.append(connection_pool_model);
        buf.append(") on ");
        if (address != null) {
            buf.append(address.getHostAddress());
            buf.append(" ");
        }
        buf.append("port: ");
        buf.append(getJDBCPort());
        return new String(buf);
    }

}
