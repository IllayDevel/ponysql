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

import com.pony.database.Database;
import com.pony.debug.*;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A multi-threaded implementation of a connection pool server.  This starts
 * a new thread for each connection made and processes each command as they
 * arrive.
 *
 * @author Tobias Downer
 */

final class MultiThreadedConnectionPoolServer implements ConnectionPoolServer {

    /**
     * If this is set to true then the server periodically outputs statistics
     * about the connections.
     */
    private static final boolean DISPLAY_STATS = false;

    /**
     * The Database parent.
     */
    private final Database database;

    /**
     * The list of all threads that were created to deal with incoming
     * commands.
     */
    private final ArrayList client_threads;


    /**
     * The Constructor.  The argument is the configuration file.
     */
    MultiThreadedConnectionPoolServer(Database database) {
        this.database = database;
        client_threads = new ArrayList();
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
        ClientThread client_thread = new ClientThread(connection);
        synchronized (client_threads) {
            client_threads.add(client_thread);
        }
        client_thread.start();
    }

    /**
     * Closes this connection pool server down.
     */
    public void close() {
        synchronized (client_threads) {
            int size = client_threads.size();
            for (int i = 0; i < size; ++i) {
                ((ClientThread) client_threads.get(i)).close();
            }
        }
    }

    // ---------- Inner classes ----------

    /**
     * This thread blocks waiting for a complete command to arrive from the
     * client it is connected to.
     */
    private class ClientThread extends Thread {

        /**
         * The ServerConnection object being serviced by this thread.
         */
        private final ServerConnection server_connection;

        /**
         * If this is set to true, the thread run method should close off.
         */
        private boolean client_closed;

        /**
         * This is set to true if we are processing a request from the client.
         */
        private boolean processing_command;

        /**
         * The Constructor.
         */
        public ClientThread(ServerConnection connection) {
            super();
//      setPriority(NORM_PRIORITY - 1);
            setName("Pony - Client Connection");

            this.server_connection = connection;
            client_closed = false;
            processing_command = false;
        }

        /**
         * Checks each connection in the 'service_connection_list' list.  If there
         * is a command pending, and any previous commands on this connection have
         * completed, then this will spawn off a new process to deal with the
         * command.
         */
        private void checkCurrentConnection() throws InterruptedException {
            try {

                // Wait if we are processing a command
                synchronized (this) {
                    while (processing_command) {
                        if (client_closed) {
                            return;
                        }
                        // Wait 2 minutes just to make sure we don't miss a poll,
                        wait(120000);
                    }
                }

                // Block until a complete command is available
                server_connection.blockForRequest();

                processing_command = true;

                database.execute(null, null, new Runnable() {
                    public void run() {

                        try {
                            // Process the next request that's pending.
                            server_connection.processRequest();
                        } catch (IOException e) {
                            Debug().writeException(Lvl.INFORMATION, e);
                        } finally {
                            // Not processing a command anymore so notify the ClientThread
                            processing_command = false;
                            synchronized (ClientThread.this) {
                                ClientThread.this.notifyAll();
                            }
                        }

                    }
                });

            } catch (IOException e) {
                // If an IOException is generated, we must remove this provider from
                // the list.
                close();

                // This happens if the connection closes.
                Debug().write(Lvl.INFORMATION, this,
                        "IOException generated while checking connections, " +
                                "removing provider.");
                Debug().writeException(Lvl.INFORMATION, e);
            }

        }

        /**
         * Call this method to stop the thread.
         */
        public synchronized void close() {
            client_closed = true;
            try {
                server_connection.close();
            } catch (Throwable e) {
                // ignore
            }
            notifyAll();
        }

        /**
         * The Runnable method of the farmer thread.
         */
        public void run() {
            while (true) {
                try {

                    boolean closed = false;
                    synchronized (this) {
                        closed = client_closed;
                    }
                    // Exit if the farmer thread has been closed...
                    if (closed == true) {
                        // Remove this thread from the list of client threads.
                        synchronized (client_threads) {
                            client_threads.remove(this);
                        }
                        return;
                    }

                    checkCurrentConnection();

                } catch (Throwable e) {
                    Debug().write(Lvl.ERROR, this, "Connection Pool Farmer Error");
                    Debug().writeException(e);
                }
            }
        }

    }

}
