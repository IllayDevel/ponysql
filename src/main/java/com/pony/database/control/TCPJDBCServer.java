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

package com.pony.database.control;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.pony.database.jdbcserver.TCPServer;

/**
 * Attaches to a DBSystem, and binds a TCP port and serves queries for JDBC
 * connections.  This object is used to programmatically create a TCP JDBC
 * server on the local machine.
 * <p>
 * Note that multiple servers can be constructed to serve the same DBSystem.
 * You can not use this object to connect a single TCP server to multiple
 * DBSystem objects.
 * <p>
 * If the underlying database is shut down then this server is also shut down.
 *
 * @author Tobias Downer
 */

public class TCPJDBCServer {

    /**
     * The default TCP port for Pony SQL Database.
     */
    private final static int DEFAULT_TCP_PORT = 9157;

    /**
     * The DBSystem object that we are serving.
     */
    private final DBSystem system;

    /**
     * An InetAddress representing the interface that server is bound to - useful
     * for multi-homed machines.  null means we bind to all interfaces.
     */
    private InetAddress bind_address;

    /**
     * The TCP port that this server is bound to.
     */
    private int tcp_port;

    /**
     * The TCPServer object that is managing the connections to this database.
     */
    private TCPServer server;


    /**
     * Constructs the TCP JDBC with the given DBSystem object, and sets the
     * inet address and TCP port that we serve the database from.
     * <p>
     * Constructing this server does not open the port to receive connections
     * from outside.  To start the JDBC server you need to call the 'start'
     * method.
     */
    public TCPJDBCServer(DBSystem system,
                         InetAddress bind_address, int tcp_port) {
        this.system = system;
        this.bind_address = bind_address;
        this.tcp_port = tcp_port;
        registerShutdownDelegate();
    }

    /**
     * Constructs the TCP JDBC with the given DBSystem object, and sets the
     * TCP port that we serve the database from.  This binds the server to all
     * interfaces on the local machine.
     * <p>
     * Constructing this server does not open the port to receive connections
     * from outside.  To start the JDBC server you need to call the 'start'
     * method.
     */
    public TCPJDBCServer(DBSystem system, int tcp_port) {
        this(system, null, tcp_port);
    }

    /**
     * Constructs the TCP JDBC with the given DBSystem object, and sets the
     * TCP port and address (for multi-homed computers) to the setting of the
     * configuration in 'system'.
     * <p>
     * Constructing this server does not open the port to receive connections
     * from outside.  To start the JDBC server you need to call the 'start'
     * method.
     */
    public TCPJDBCServer(DBSystem system) {
        this.system = system;

        DBConfig config = system.getConfig();

        int jdbc_port = DEFAULT_TCP_PORT;
        InetAddress interface_address = null;

        // Read the JDBC config properties.
        String jdbc_port_str = config.getValue("jdbc_server_port");
        String interface_addr_str = config.getValue("jdbc_server_address");

        if (jdbc_port_str != null) {
            try {
                jdbc_port = Integer.parseInt(jdbc_port_str);
            } catch (Exception e) {
                throw new RuntimeException("Unable to parse 'jdbc_server_port'");
            }
        }
        if (interface_addr_str != null) {
            try {
                interface_address = InetAddress.getByName(interface_addr_str);
            } catch (UnknownHostException e) {
                throw new RuntimeException("Unknown host: " + e.getMessage());
            }
        }

        // Set up this port and bind address
        this.tcp_port = jdbc_port;
        this.bind_address = interface_address;

        registerShutdownDelegate();
    }

    /**
     * Registers the delegate that closes this server when the database
     * shuts down.
     */
    private void registerShutdownDelegate() {
        system.getDatabase().registerShutDownDelegate(() -> {
            if (server != null) {
                stop();
            }
        });
    }

    /**
     * Starts the server and binds it to the given port.  This method will start
     * a new thread that listens for incoming connections.
     */
    public synchronized void start() {
        if (server == null) {
            server = new TCPServer(system.getDatabase());
            server.start(bind_address, tcp_port, "multi_threaded");
        } else {
            throw new RuntimeException(
                    "'start' method called when a server was already started.");
        }
    }

    /**
     * Stops the server running on the given port.  This method will stop any
     * threads that are listening for incoming connections.
     * <p>
     * Note that this does NOT close the underlying DBSystem object.  The
     * DBSystem object must be closed separately.
     */
    public synchronized void stop() {
        if (server != null) {
            server.close();
            server = null;
        } else {
            throw new RuntimeException(
                    "'stop' method called when no server was started.");
        }
    }


    /**
     * Returns a string that contains some information about the server that
     * is running.
     */
    public String toString() {
        return server.toString();
    }

}
