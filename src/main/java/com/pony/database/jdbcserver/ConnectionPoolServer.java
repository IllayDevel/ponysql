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

/**
 * An interface for the connection pool for a server.  This is the API for
 * a service that accepts connections via 'addConnection', waits for the
 * connection to make a request, and dispatch the request as appropriate to
 * the database engine.
 * <p>
 * This interface is used to provide different implementations for command
 * dispatching mechanisms, such as a thread per TCP user, one thread per
 * TCP connection set, UDP, etc.
 *
 * @author Tobias Downer
 */

interface ConnectionPoolServer {

    /**
     * Connects a new ServerConnection into the pool of connections to clients
     * that this server maintains.
     */
    void addConnection(ServerConnection connection);

    /**
     * Closes this connection pool server down.
     */
    void close();

}
