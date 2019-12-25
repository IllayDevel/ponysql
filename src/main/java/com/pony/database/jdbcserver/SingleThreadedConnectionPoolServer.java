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

import com.pony.database.User;
import com.pony.database.Database;
import com.pony.database.DatabaseSystem;
import com.pony.debug.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.ResourceBundle;

/**
 * A generic database server class that provides a thread that dispatches
 * commands to the underlying database.  This class only provides a framework
 * for creating a server.  It doesn't provide any implementation
 * specifics for protocols.
 * <p>
 * An TCP implementation of this class would wait for connections and then
 * create a ServerConnection implementation and feed it into the pool for
 * processing.  This object will then poll the ServerConnection until a
 * command is pending, and then dispatch the command to a database worker
 * thread.
 * <p>
 * This object will ping the clients every so often to see if they are alive.
 *
 * @author Tobias Downer
 */

final class SingleThreadedConnectionPoolServer
        implements ConnectionPoolServer {

    /**
     * The number of milliseconds between client pings.
     * NOTE: Should this be a configurable variable in the '.conf' file?
     * (45 seconds)
     */
    private static final int PING_BREAK = 45 * 1000;  //4 * 60 * 1000;

    /**
     * If this is set to true then the server periodically outputs statistics
     * about the connections.
     */
    private static final boolean DISPLAY_STATS = false;

    /**
     * The Database context.
     */
    private final Database database;

    /**
     * The list of ServerConnection objects that are pending to be added into the
     * current service provider list next time it is checked.
     */
    private final ArrayList pending_connections_list;

    /**
     * The ServerFarmer object that polls for information from the clients and
     * dispatches the request to the worker threads.
     */
    private final ServerFarmer farmer;


    /**
     * The Constructor.  The argument is the configuration file.
     */
    SingleThreadedConnectionPoolServer(Database database) {
        this.database = database;
        pending_connections_list = new ArrayList();
        // Create the farmer thread that services all the connections.
        farmer = new ServerFarmer();
        farmer.start();
    }

    /**
     * Returns a DebugLogger object that we can log debug messages to.
     */
    public final DebugLogger Debug() {
        return database.Debug();
    }

    /**
     * Connects a new ServerConnection into the pool of connections to clients
     * that this server maintains.  We then cycle through these connections
     * determining whether any commands are pending.  If a command is pending
     * we spawn off a worker thread to do the task.
     */
    public void addConnection(ServerConnection connection) {
        synchronized (pending_connections_list) {
            pending_connections_list.add(connection);
        }
    }

    /**
     * Closes this connection pool server down.
     */
    public void close() {
        farmer.close();
    }

    // ---------- Inner classes ----------

    /**
     * This thread is a low priority thread that checks all the current service
     * providers periodically to determine if there's any commands pending.
     */
    private class ServerFarmer extends Thread {

        /**
         * The list of ServerConnection objects that are currently being serviced
         * by this server.
         */
        private final ArrayList server_connections_list;

        /**
         * Staticial information collected.
         */
        private int stat_display = 0;

        /**
         * If this is set to true, then the farmer run method should close off.
         */
        private boolean farmer_closed;

        /**
         * The number of milliseconds to wait between each poll of the 'available'
         * method of the socket.  This value is determined by the configuration
         * file during initialization.
         */
        private final int poll_wait_time;


        /**
         * The Constructor.
         */
        public ServerFarmer() {
            super();
//      setPriority(NORM_PRIORITY - 1);

            // The time in ms between each poll of the 'available' method.
            // Default is '3 ms'
            poll_wait_time = 3;

            server_connections_list = new ArrayList();
            farmer_closed = false;
        }

        /**
         * Establishes a connection with any current pending connections in the
         * 'pending_connections_list'.
         */
        private void establishPendingConnections() throws IOException {
            synchronized (pending_connections_list) {
                int len = pending_connections_list.size();
                // Move all pending connections into the current connections list.
                for (int i = 0; i < len; ++i) {
                    // Get the connection and create the new connection state
                    ServerConnection connection =
                            (ServerConnection) pending_connections_list.remove(0);
                    server_connections_list.add(new ServerConnectionState(connection));
                }
            }
        }

        /**
         * Checks each connection in the 'service_connection_list' list.  If there
         * is a command pending, and any previous commands on this connection have
         * completed, then this will spawn off a new process to deal with the
         * command.
         */
        private void checkCurrentConnections() {
            int len = server_connections_list.size();
            for (int i = len - 1; i >= 0; --i) {
                ServerConnectionState connection_state =
                        (ServerConnectionState) server_connections_list.get(i);
                try {
                    // Is this connection not currently processing a command?
                    if (!connection_state.isProcessingRequest()) {
                        ServerConnection connection = connection_state.getConnection();
                        // Does this connection have a request pending?
                        if (connection_state.hasPendingCommand() ||
                                connection.requestPending()) {
                            // Set that we have a pending command
                            connection_state.setPendingCommand();
                            connection_state.setProcessingRequest();

                            final ServerConnectionState current_state = connection_state;

//              // Execute this on a database worker thread.
//              final User conn_user = connection.getUser();
//              DatabaseSystem.execute(conn_user, connection.getDatabase(),
//                                     new Runnable() {
                            database.execute(null, null, new Runnable() {
                                public void run() {

                                    try {
                                        // Process the next request that's pending.
                                        current_state.getConnection().processRequest();
                                    } catch (IOException e) {
                                        Debug().writeException(Lvl.INFORMATION, e);
                                    } finally {
                                        // Then clear the state
                                        // This makes sure that this provider may accept new
                                        // commands again.
                                        current_state.clearInternal();
                                    }

                                }
                            });

                        } // if (provider_state.hasPendingCommand() ....
                    } // if (!provider_state.isProcessRequest())
                } catch (IOException e) {
                    // If an IOException is generated, we must remove this provider from
                    // the list.
                    try {
                        connection_state.getConnection().close();
                    } catch (IOException e2) { /* ignore */ }
                    server_connections_list.remove(i);

                    // This happens if the connection closes.
                    Debug().write(Lvl.INFORMATION, this,
                            "IOException generated while checking connections, " +
                                    "removing provider.");
                    Debug().writeException(Lvl.INFORMATION, e);
                }
            }
        }

        /**
         * Performs a ping on a single random connection.  If the ping fails then
         * the connection is closed.
         */
        private void doPings() {
            int len = server_connections_list.size();
            if (len == 0) {
                if (DISPLAY_STATS) {
                    System.out.print("[TCPServer Stats] ");
                    System.out.println("Ping tried but no connections.");
                }
                return;
            }
            int i = (int) (Math.random() * len);

            if (DISPLAY_STATS) {
                System.out.print("[TCPServer Stats] ");
                System.out.print("Pinging client ");
                System.out.print(i);
                System.out.println(".");
            }

            final ServerConnectionState connection_state =
                    (ServerConnectionState) server_connections_list.get(i);

            // Is this provider not currently processing a command?
            if (!connection_state.isProcessingRequest()) {
                // Don't let a command interrupt the ping.
                connection_state.setProcessingRequest();

                // ISSUE: Pings are executed under 'null' user and database...
                database.execute(null, null, new Runnable() {
                    public void run() {
                        try {
                            // Ping the client? - This closes the provider if the
                            // ping fails.
                            connection_state.getConnection().ping();
                        } catch (IOException e) {
                            // Close connection
                            try {
                                connection_state.getConnection().close();
                            } catch (IOException e2) { /* ignore */ }
                            Debug().write(Lvl.ALERT, ServerFarmer.this,
                                    "Closed because ping failed.");
                            Debug().writeException(Lvl.ALERT, e);
                        } finally {
                            connection_state.clearProcessingRequest();
                        }
                    }
                });

            } // if (!provider_state.isProcessRequest())
        }

        /**
         * Displays statistics about the server.
         */
        private void displayStatistics() {
            if (DISPLAY_STATS) {
                if (stat_display == 0) {
                    stat_display = 500;
                    System.out.print("[TCPServer Stats] ");
                    int commands_run = 0;
                    System.out.print(commands_run);
                    System.out.print(" run, ");
                    int commands_waited = 0;
                    System.out.print(commands_waited);
                    System.out.print(" wait, ");
                    System.out.print(server_connections_list.size());
                    System.out.print(" worker count");
                    System.out.println();
                } else {
                    --stat_display;
                }
            }
        }

        /**
         * Call this method to stop the farmer thread.
         */
        public synchronized void close() {
            farmer_closed = true;
        }

        /**
         * The Runnable method of the farmer thread.
         */
        public void run() {
            int yield_count = 0;
            long do_ping_time = System.currentTimeMillis() + PING_BREAK;
            int ping_count = 200;

            final int method_poll_wait_time = poll_wait_time;

            Debug().write(Lvl.MESSAGE, this,
                    "Polling frequency: " + method_poll_wait_time + "ms.");

            while (true) {
                try {

                    // First, determine if there are any pending service providers
                    // waiting to be established.
                    if (pending_connections_list.size() > 0) {
                        establishPendingConnections();
                    }
                    checkCurrentConnections();

                    // Is it time to ping the clients?
                    --ping_count;
                    if (ping_count <= 0) {
                        ping_count = 2000;
                        long current_time = System.currentTimeMillis();
                        if (current_time > do_ping_time) {
                            // Randomly ping
                            doPings();
                            do_ping_time = current_time + PING_BREAK;
                        }
                    }

                    if (yield_count <= 0) {
                        synchronized (this) {
                            // Wait for 3ms to give everything room to breath
                            wait(method_poll_wait_time);
                            yield_count = 3;
                        }
                    } else {
                        synchronized (this) {
                            // Exit if the farmer thread has been closed...
                            if (farmer_closed == true) {
                                return;
                            }
                        }
                        Thread.yield();
                        --yield_count;
                    }

                    // Print out connection statistics every so often
                    displayStatistics();

                } catch (Throwable e) {
                    Debug().write(Lvl.ERROR, this, "Connection Pool Farmer Error");
                    Debug().writeException(e);

                    // Wait for two seconds (so debug log isn't spammed)
                    synchronized (this) {
                        try {
                            wait(2000);
                        } catch (InterruptedException e2) { /* ignore */ }
                    }

                }
            }
        }

    }


    /**
     * This contains state information about a ServerConnection that is being
     * maintained by the server.
     */
    private static final class ServerConnectionState {

        /**
         * The local variables.
         */
        private final ServerConnection connection;
        //    private boolean is_establish;
        private boolean is_processing_request;
        private boolean is_pending_command;
        private boolean is_ping_client;

        /**
         * The Constructor.
         */
        ServerConnectionState(ServerConnection connection) {
            this.connection = connection;
            clearInternal();
//      is_establish = true;
        }

        /**
         * Sets the various states to true.
         */
        public synchronized void setProcessingRequest() {
            is_processing_request = true;
        }

        public synchronized void setPendingCommand() {
            is_pending_command = true;
        }

        public synchronized void setPingClient() {
            is_ping_client = true;
        }


        /**
         * Clears the internal state.
         */
        public synchronized void clearInternal() {
            is_processing_request = false;
            is_pending_command = false;
//      is_establish = false;
            is_ping_client = false;
        }

        /**
         * Clears the flag that says we are processing a request.
         */
        public synchronized void clearProcessingRequest() {
            is_processing_request = false;
        }

        /**
         * Queries the internal state.
         */
        public synchronized ServerConnection getConnection() {
            return connection;
        }

        public synchronized boolean isProcessingRequest() {
            return is_processing_request;
        }

        public synchronized boolean hasPendingCommand() {
            return is_pending_command;
        }

        //    public synchronized boolean isEstablishConnection() {
//      return is_establish;
//    }
        public synchronized boolean isPingClient() {
            return is_ping_client;
        }

    }

}
