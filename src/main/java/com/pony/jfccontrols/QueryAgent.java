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

package com.pony.jfccontrols;

import java.util.ArrayList;
import java.awt.*;
import java.sql.*;
import javax.swing.*;

/**
 * A class that is an agent for queries from the client environment to the
 * server.  All locked swing event dispatched JDBC queries should go through
 * this class.  These are queries where the user does something which results
 * in a query being executed on the server, and must then wait for the result
 * to be received.
 * <p>
 * This class provides a mechanism which allows the client not to block
 * indeffinately on the event dispatcher thread.  This means we can give
 * feedback to the user if a query is taking a long time (progress bar,
 * hour-glass, etc) and components will repaint correctly.  It also allows us
 * to cancel any query in progress (because the event dispatcher isn't locked
 * we can handle UI events and the interface won't be frozen).
 * <p>
 * We acheive this behaviour with a hack of the system EventQueue (the same way
 * modality works in swing.JInternalFrame).  The low down is, we emulate the
 * Java event dispatcher inner loop so that all AWT events (repaints/
 * component events/etc) are dispatched in our own controlled loop that is
 * blocked with respect to the callee.  (Find this blocking behaviour in
 * SwingBlockUtil)
 * <p>
 * I consider this a mild hack.  This class may be incompatible with future
 * versions of Java if the AWT event mechanism is altered.  It may also not
 * work happily with non-Sun based implementations of Java.
 * <p>
 * NOTE: Other ways of acheiving non-AWT locking queries is with a delegate
 *  implementation.  The method that executes the query returns immediately
 *  and the result is sent to a delegate.  While this system sounds nice in
 *  theory, it's not pretty in practice.  Especially if you need to execute
 *  many queries in a specific sequence.  Also, handling exceptions is a
 *  nightmare with this idea.
 *
 * @author Tobias Downer
 */

public class QueryAgent {

    /**
     * The JDBC Connection object for the connection to the database.
     */
    private Connection connection = null;

    /**
     * The utility for blocking the swing event dispatch thread.
     */
    private SwingBlockUtil block_util;

    /**
     * The thread we use to send commands to the JDBC connection.
     */
    private QueryThread query_thread;

    /**
     * This represents the state of the result of the query.  Either 'n' (none),
     * 'e' (exception), 'r' (result), 'f' (finished), or 'c' (cancelled).
     */
    private char query_finished = 'f';

    /**
     * If an exception was thrown, the SQLException.
     */
    private SQLException sql_exception;

    /**
     * If a ResultSet was obtained from the query, the ResultSet.
     */
    private ResultSet result_set;

    /**
     * Constructs the query agent.
     */
    public QueryAgent(Connection connection) {
        this.connection = connection;
        block_util = new SwingBlockUtil();
        query_thread = new QueryThread();
        query_thread.start();
    }

    /**
     * Returns the connection for the JDBC interface.
     */
    public Connection connection() {
        return connection;
    }

    /**
     * Executes a query, blocking until a response from the server has been
     * received.  This will send the command to the server on the QueryThread
     * and emulates the event dispatch thread so AWT events can still be
     * processed.
     * <p>
     * This is based on the 'setModal' method found in JInternalFrame.  It
     * is up to the developer to block the user interface elements from being
     * used while we are waiting for a query result.
     * <p>
     * Throws an InterruptedException if the query is cancelled via the
     * 'cancelQuery' method.
     */
    public ResultSet executeQuery(Query query)
            throws SQLException, InterruptedException {

        if (!SwingUtilities.isEventDispatchThread()) {
            throw new Error("Not on the event dispatcher.");
        } else if (query_finished != 'f') {
            // This situation would occur when a component generates an event (such
            // as the user pressing a button) that performs a query.  Therefore
            // there are multi-levels of queries being executed.
            throw new Error("Can't nest queries.");
        }

        // Set the new statement to be executed,
        PreparedStatement statement =
                connection.prepareStatement(query.getString());
        for (int i = 0; i < query.parameterCount(); ++i) {
            statement.setObject(i + 1, query.getParameter(i));
        }

        query_thread.addStatement(statement);
        query_finished = 'n';
        // Block until statement finished or cancelled.
        block_util.block();

        if (query_finished == 'e') {
            SQLException e = sql_exception;
            sql_exception = null;
            query_finished = 'f';
            throw e;
        } else if (query_finished == 'r') {
            ResultSet rs = result_set;
            result_set = null;
            query_finished = 'f';
            return rs;
        } else if (query_finished == 'c') {
            query_finished = 'f';
            throw new InterruptedException("Query Cancelled");
        } else {
            char old_state = query_finished;
            query_finished = 'f';
            throw new Error("Unknown query state: " + old_state);
        }

    }

    /**
     * Cancels any query that is currently being executed by this agent.
     * This will cause the 'executeQuery' method to throw an
     * InterruptedException.
     */
    public void cancelQuery() {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                if (query_finished != 'f') {
                    query_finished = 'c';
                    block_util.unblock();
                }
            }
        });
    }

    /**
     * Returns the current system EventQueue.
     */
    private EventQueue eventQueue() {
        return Toolkit.getDefaultToolkit().getSystemEventQueue();
    }

    /**
     * Returns true if the query is done or not yet.
     */
    private boolean isQueryDone() {
        return query_finished != 'n';
    }

    /**
     * This is called when a query has finished and an SQLException was
     * thrown.
     * <p>
     * Called from the QueryThread.
     */
    private void notifyException(
            final PreparedStatement statement, final SQLException e) {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                if (query_finished != 'f') {
                    sql_exception = e;
                    query_finished = 'e';
                    block_util.unblock();
                }
            }
        });
    }

    /**
     * This is called when a query has finished and a valid ResultSet was
     * returned as the result.
     * <p>
     * Called from the QueryThread.
     */
    private void notifyComplete(
            final PreparedStatement statement, final ResultSet result_set) {
        SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                if (query_finished != 'f') {
                    QueryAgent.this.result_set = result_set;
                    query_finished = 'r';
                    block_util.unblock();
                }
            }
        });
    }

    // ---------- Inner classes ----------

    /**
     * The query thread.
     */
    private class QueryThread extends Thread {

        // The list of statements pending to be executed,
        private ArrayList statements = new ArrayList();

        private QueryThread() {
            super();
            setDaemon(true);
            setName("Pony - Query Agent");
        }

        /**
         * Sets the PreparedStatement that we want to execute.
         */
        private synchronized void addStatement(PreparedStatement statement) {
            statements.add(statement);
            notifyAll();
        }

        public void run() {
            while (true) {
                try {
                    PreparedStatement to_exec = null;
                    synchronized (this) {
                        while (statements.size() == 0) {
                            wait();
                        }
                        to_exec = (PreparedStatement) statements.remove(0);
                    }
                    try {
                        // 'to_exec' is the statement to execute next,
                        ResultSet result_set = to_exec.executeQuery();
                        notifyComplete(to_exec, result_set);
                    } catch (SQLException e) {
                        notifyException(to_exec, e);
                    }
                } catch (Throwable e) {
                    System.err.println("Exception during QueryThread: " +
                            e.getMessage());
                    e.printStackTrace(System.err);
                }
            }
        }

    }

    // ---------- Static methods ----------

    private static QueryAgent query_agent = null;

    /**
     * Initialises the QueryAgent with the given JDBC Connection.
     */
    public static void initAgent(Connection connection) {
        if (query_agent == null) {
            query_agent = new QueryAgent(connection);
        }
    }

    /**
     * Returns the QueryAgent for this VM.
     */
    public static QueryAgent getDefaultAgent() {
        return query_agent;
    }

    /**
     * Executes a query on the default query agent for this VM.  This must be
     * called on the swing event dispatcher thread.
     * <p>
     * This will block until the server has responded to the query and a result
     * has been obtained.  While this method does block, it still will service
     * events on the event dispatcher queue.  This means, UI elements will still
     * work (buttons/text fields/etc) at the same time the server is fetching
     * the result.  And, we still have blocking behaviour for the callee.
     * <p>
     * What this ultimately means, is that components can be repainted and
     * we can have animations indicating the progress (feedback from the UI to
     * the user that it hasn't frozen up) and buttons to cancel the query if
     * it's taking too long.
     */
    public static ResultSet execute(Query query)
            throws SQLException, InterruptedException {
        return getDefaultAgent().executeQuery(query);
    }

    /**
     * Cancels the query that is currently being executed (if any).  This must
     * be called on the swing event dispatcher thread.
     * <p>
     * This will throw an InterruptedException from the 'execute' method if it
     * is waiting.
     */
    public static void cancel() {
        getDefaultAgent().cancelQuery();
    }

}
