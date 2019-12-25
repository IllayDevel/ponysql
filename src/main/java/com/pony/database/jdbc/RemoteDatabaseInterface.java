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
import java.util.Vector;

import com.pony.database.global.ColumnDescription;
import com.pony.database.global.ObjectTransfer;
import com.pony.util.ByteArrayUtil;


/**
 * An abstract implementation of DatabaseInterface that retrieves information
 * from a remote server host.  The actual implementation of the communication
 * protocol is left to the derived classes.
 *
 * @author Tobias Downer
 */

abstract class RemoteDatabaseInterface
        implements DatabaseInterface, ProtocolConstants {

    /**
     * The thread that dispatches commands to the server.  This is created and
     * started after the 'login' method is called.  This can handle concurrent
     * queries through the protocol pipe.
     */
    private ConnectionThread connection_thread;

    /**
     * A DatabaseCallBack implementation that is notified of all events that
     * are received from the database.
     */
    private DatabaseCallBack database_call_back;


    /**
     * Writes the exception to the JDBC log stream.
     */
    private static void logException(Throwable e) {
        PrintWriter out = null;
//#IFDEF(NO_1.1)
        out = DriverManager.getLogWriter();
//#ENDIF
        if (out != null) {
            e.printStackTrace(out);
        }
//    else {
//      e.printStackTrace(System.err);
//    }
    }


    // ---------- Abstract methods ----------

    /**
     * Writes the given command to the server.  The way the command is written
     * is totally network layer dependent.
     */
    abstract void writeCommandToServer(byte[] command, int offset, int length)
            throws IOException;

    /**
     * Blocks until the next command is received from the server.  The way this
     * is implemented is network layer dependant.
     */
    abstract byte[] nextCommandFromServer(int timeout) throws IOException;

    /**
     * Closes the connection.
     */
    abstract void closeConnection() throws IOException;


    // ---------- Implemented from DatabaseInterface ----------

    public boolean login(String default_schema, String user, String password,
                         DatabaseCallBack call_back) throws SQLException {

        try {

            // Do some handshaking,
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);

            // Write out the magic number
            out.writeInt(0x0ced007);
            // Write out the JDBC driver version
            out.writeInt(MDriver.DRIVER_MAJOR_VERSION);
            out.writeInt(MDriver.DRIVER_MINOR_VERSION);
            byte[] arr = bout.toByteArray();
            writeCommandToServer(arr, 0, arr.length);

            byte[] response = nextCommandFromServer(0);

//      printByteArray(response);

            int ack = ByteArrayUtil.getInt(response, 0);
            if (ack == ACKNOWLEDGEMENT) {

                // History of server versions (inclusive)
                //    Engine version |  server_version
                //  -----------------|-------------------
                //    0.00 - 0.91    |  0
                //    0.92 -         |  1
                //  -----------------|-------------------

                // Server version defaults to 0
                // Server version 0 is for all versions of the engine previous to 0.92
                int server_version = 0;
                // Is there anything more to read?
                if (response.length > 4 && response[4] == 1) {
                    // Yes so read the server version
                    server_version = ByteArrayUtil.getInt(response, 5);
                }

                // Send the username and password to the server
                // SECURITY: username/password sent as plain text.  This is okay
                //   if we are connecting to localhost, but not good if we connecting
                //   over the internet.  We could encrypt this, but it would probably
                //   be better if we put the entire stream through an encyption
                //   protocol.

                bout.reset();
                out.writeUTF(default_schema);
                out.writeUTF(user);
                out.writeUTF(password);
                arr = bout.toByteArray();
                writeCommandToServer(arr, 0, arr.length);

                response = nextCommandFromServer(0);
                int result = ByteArrayUtil.getInt(response, 0);
                if (result == USER_AUTHENTICATION_PASSED) {

                    // Set the call_back,
                    this.database_call_back = call_back;

                    // User authentication passed so we successfully logged in now.
                    connection_thread = new ConnectionThread();
                    connection_thread.start();
                    return true;

                } else if (result == USER_AUTHENTICATION_FAILED) {
                    throw new SQLLoginException("User Authentication failed.");
                } else {
                    throw new SQLException("Unexpected response.");
                }

            } else {
                throw new SQLException("No acknowledgement received from server.");
            }

        } catch (IOException e) {
            logException(e);
            throw new SQLException("IOException: " + e.getMessage());
        }

    }


    public void pushStreamableObjectPart(byte type, long object_id,
                                         long object_length, byte[] buf, long offset, int length)
            throws SQLException {
        try {
            // Push the object part
            int dispatch_id = connection_thread.pushStreamableObjectPart(
                    type, object_id, object_length, buf, offset, length);
            // Get the response
            ServerCommand command =
                    connection_thread.getCommand(MDriver.QUERY_TIMEOUT, dispatch_id);
            // If command == null then we timed out
            if (command == null) {
                throw new SQLException("Query timed out after " +
                        MDriver.QUERY_TIMEOUT + " seconds.");
            }

            DataInputStream din = new DataInputStream(command.getInputStream());
            int status = din.readInt();

            // If failed report the error.
            if (status == FAILED) {
                throw new SQLException("Push object failed: " + din.readUTF());
            }

        } catch (IOException e) {
            logException(e);
            throw new SQLException("IO Error: " + e.getMessage());
        }

    }


    public QueryResponse execQuery(SQLQuery sql) throws SQLException {

        try {
            // Execute the query
            int dispatch_id = connection_thread.executeQuery(sql);
            // Get the response
            ServerCommand command =
                    connection_thread.getCommand(MDriver.QUERY_TIMEOUT, dispatch_id);
            // If command == null then we timed out
            if (command == null) {
                throw new SQLException("Query timed out after " +
                        MDriver.QUERY_TIMEOUT + " seconds.");
            }

            DataInputStream in = new DataInputStream(command.getInputStream());

            // Query response protocol...
            int status = in.readInt();
            if (status == SUCCESS) {
                final int result_id = in.readInt();
                final int query_time = in.readInt();
                final int row_count = in.readInt();
                final int col_count = in.readInt();
                final ColumnDescription[] col_list = new ColumnDescription[col_count];
                for (int i = 0; i < col_count; ++i) {
                    col_list[i] = ColumnDescription.readFrom(in);
                }

                return new QueryResponse() {
                    public int getResultID() {
                        return result_id;
                    }

                    public int getQueryTimeMillis() {
                        return query_time;
                    }

                    public int getRowCount() {
                        return row_count;
                    }

                    public int getColumnCount() {
                        return col_count;
                    }

                    public ColumnDescription getColumnDescription(int n) {
                        return col_list[n];
                    }

                    public String getWarnings() {
                        return "";
                    }
                };

            } else if (status == EXCEPTION) {
                int db_code = in.readInt();
                String message = in.readUTF();
                String stack_trace = in.readUTF();
//        System.out.println("**** DUMP OF SERVER STACK TRACE OF ERROR:");
//        System.out.println(stack_trace);
//        System.out.println("**** ----------");
                throw new MSQLException(message, null, db_code, stack_trace);
            } else if (status == AUTHENTICATION_ERROR) {
                // Means we could perform the query because user doesn't have enough
                // rights.
                String access_type = in.readUTF();
                String table_name = in.readUTF();
                throw new SQLException("User doesn't have enough privs to " +
                        access_type + " table " + table_name);
            } else {
//        System.err.println(status);
//        int count = in.available();
//        for (int i = 0; i < count; ++i) {
//          System.err.print(in.read() + ", ");
//        }
                throw new SQLException("Illegal response code from server.");
            }

        } catch (IOException e) {
            logException(e);
            throw new SQLException("IO Error: " + e.getMessage());
        }

    }

    public ResultPart getResultPart(int result_id, int start_row, int count_rows)
            throws SQLException {

        try {

            // Get the first few rows of the result..
            int dispatch_id = connection_thread.getResultPart(result_id,
                    start_row, count_rows);

            // Get the response
            ServerCommand command =
                    connection_thread.getCommand(MDriver.QUERY_TIMEOUT, dispatch_id);
            // If command == null then we timed out
            if (command == null) {
                throw new SQLException("Downloading result part timed out after " +
                        MDriver.QUERY_TIMEOUT + " seconds.");
            }

            // Wrap around a DataInputStream
            DataInputStream din = new DataInputStream(command.getInputStream());
            int status = din.readInt();

            if (status == SUCCESS) {
                // Return the contents of the response.
                int col_count = din.readInt();
                int size = count_rows * col_count;
                ResultPart list = new ResultPart(size);
                for (int i = 0; i < size; ++i) {
                    list.addElement(ObjectTransfer.readFrom(din));
                }
                return list;
            } else if (status == EXCEPTION) {
                int db_code = din.readInt();
                String message = din.readUTF();
                String stack_trace = din.readUTF();
//        System.out.println("**** DUMP OF SERVER STACK TRACE OF ERROR:");
//        System.out.println(stack_trace);
//        System.out.println("**** ----------");
                throw new SQLException(message, null, db_code);
            } else {
                throw new SQLException("Illegal response code from server.");
            }

        } catch (IOException e) {
            logException(e);
            throw new SQLException("IO Error: " + e.getMessage());
        }

    }


    public void disposeResult(int result_id) throws SQLException {
        try {
            int dispatch_id = connection_thread.disposeResult(result_id);
            // Get the response
            ServerCommand command =
                    connection_thread.getCommand(MDriver.QUERY_TIMEOUT, dispatch_id);
            // If command == null then we timed out
            if (command == null) {
                throw new SQLException("Dispose result timed out after " +
                        MDriver.QUERY_TIMEOUT + " seconds.");
            }

            // Check the dispose was successful.
            DataInputStream din = new DataInputStream(command.getInputStream());
            int status = din.readInt();

            // If failed report the error.
            if (status == FAILED) {
                throw new SQLException("Dispose failed: " + din.readUTF());
            }

        } catch (IOException e) {
            logException(e);
            throw new SQLException("IO Error: " + e.getMessage());
        }
    }


    public StreamableObjectPart getStreamableObjectPart(int result_id,
                                                        long streamable_object_id, long offset, int len) throws SQLException {
        try {
            int dispatch_id = connection_thread.getStreamableObjectPart(result_id,
                    streamable_object_id, offset, len);
            ServerCommand command =
                    connection_thread.getCommand(MDriver.QUERY_TIMEOUT, dispatch_id);
            // If command == null then we timed out
            if (command == null) {
                throw new SQLException("getStreamableObjectPart timed out after " +
                        MDriver.QUERY_TIMEOUT + " seconds.");
            }

            DataInputStream din = new DataInputStream(command.getInputStream());
            int status = din.readInt();

            if (status == SUCCESS) {
                // Return the contents of the response.
                int contents_size = din.readInt();
                byte[] buf = new byte[contents_size];
                din.readFully(buf, 0, contents_size);
                return new StreamableObjectPart(buf);
            } else if (status == EXCEPTION) {
                int db_code = din.readInt();
                String message = din.readUTF();
                String stack_trace = din.readUTF();
                throw new SQLException(message, null, db_code);
            } else {
                throw new SQLException("Illegal response code from server.");
            }

        } catch (IOException e) {
            logException(e);
            throw new SQLException("IO Error: " + e.getMessage());
        }
    }


    public void disposeStreamableObject(int result_id, long streamable_object_id)
            throws SQLException {
        try {
            int dispatch_id = connection_thread.disposeStreamableObject(
                    result_id, streamable_object_id);
            ServerCommand command =
                    connection_thread.getCommand(MDriver.QUERY_TIMEOUT, dispatch_id);
            // If command == null then we timed out
            if (command == null) {
                throw new SQLException("disposeStreamableObject timed out after " +
                        MDriver.QUERY_TIMEOUT + " seconds.");
            }

            DataInputStream din = new DataInputStream(command.getInputStream());
            int status = din.readInt();

            // If failed report the error.
            if (status == FAILED) {
                throw new SQLException("Dispose failed: " + din.readUTF());
            }

        } catch (IOException e) {
            logException(e);
            throw new SQLException("IO Error: " + e.getMessage());
        }
    }


    public void dispose() throws SQLException {
        try {
            int dispatch_id = connection_thread.sendCloseCommand();
//      // Get the response
//      ServerCommand command =
//            connection_thread.getCommand(MDriver.QUERY_TIMEOUT, dispatch_id);
            closeConnection();
        } catch (IOException e) {
            logException(e);
            throw new SQLException("IO Error: " + e.getMessage());
        }
    }

    // ---------- Inner classes ----------

    /**
     * The connection thread that can dispatch commands concurrently through the
     * in/out pipe.
     */
    private class ConnectionThread extends Thread {

        /**
         * The command to write out to the server.
         */
        private final MByteArrayOutputStream com_bytes;
        private final DataOutputStream com_data;

        /**
         * Running dispatch id values which we use as a unique key.
         */
        private int running_dispatch_id = 1;

        /**
         * Set to true when the thread is closed.
         */
        private final boolean thread_closed;

        /**
         * The list of commands received from the server that are pending to be
         * processed (ServerCommand).
         */
        private Vector commands_list;


        /**
         * Constructs the connection thread.
         */
        ConnectionThread() throws IOException {
            setDaemon(true);
            setName("Pony - Connection Thread");
            com_bytes = new MByteArrayOutputStream();
            com_data = new DataOutputStream(com_bytes);

            commands_list = new Vector();
            thread_closed = false;
        }

        // ---------- Utility ----------

        /**
         * Returns a unique dispatch id number for a command.
         */
        private int nextDispatchID() {
            return running_dispatch_id++;
        }

        /**
         * Blocks until a response from the server has been received with the
         * given dispatch id.  It waits for 'timeout' seconds and if the response
         * hasn't been received by then returns null.
         */
        ServerCommand getCommand(int timeout, int dispatch_id)
                throws SQLException {
            final long time_in = System.currentTimeMillis();
            final long time_out_high = time_in + ((long) timeout * 1000);

            synchronized (commands_list) {

                if (commands_list == null) {
                    throw new SQLException("Connection to server closed");
                }

                while (true) {

                    for (int i = 0; i < commands_list.size(); ++i) {
                        ServerCommand command = (ServerCommand) commands_list.elementAt(i);
                        if (command.dispatchID() == dispatch_id) {
                            commands_list.removeElementAt(i);
                            return command;
                        }
                    }

                    // Return null if we haven't received a response in the timeout
                    // period.
                    if (timeout != 0 &&
                            System.currentTimeMillis() > time_out_high) {
                        return null;
                    }

                    // Wait a second.
                    try {
                        commands_list.wait(1000);
                    } catch (InterruptedException e) { /* ignore */ }

                } // while (true)

            } // synchronized

        }


        // ---------- Server request methods ----------

        /**
         * Flushes the command in 'com_bytes' to the server.
         */
        private synchronized void flushCommand() throws IOException {
            // We flush the size of the command string followed by the command
            // itself to the server.  This format allows us to implement a simple
            // non-blocking command parser on the server.
            writeCommandToServer(com_bytes.getBuffer(), 0, com_bytes.size());
            com_bytes.reset();
        }

        /**
         * Pushes a part of a streamable object onto the server.  Used in
         * preparation to executing queries containing large objects.
         */
        synchronized int pushStreamableObjectPart(byte type, long object_id,
                                                  long object_length, byte[] buf, long offset, int length)
                throws IOException {
            int dispatch_id = nextDispatchID();
            com_data.writeInt(PUSH_STREAMABLE_OBJECT_PART);
            com_data.writeInt(dispatch_id);
            com_data.writeByte(type);
            com_data.writeLong(object_id);
            com_data.writeLong(object_length);
            com_data.writeInt(length);
            com_data.write(buf, 0, length);
            com_data.writeLong(offset);
            flushCommand();

            return dispatch_id;
        }

        /**
         * Sends a command to the server to process a query.  The response from
         * the server will contain a 'result_id' that is a unique number for
         * refering to the result.  It also contains information about the columns
         * in the table, and the total number of rows in the result.
         * <p>
         * Returns the dispatch id key for the response from the server.
         */
        synchronized int executeQuery(SQLQuery sql) throws IOException {
            int dispatch_id = nextDispatchID();
            com_data.writeInt(QUERY);
            com_data.writeInt(dispatch_id);
            sql.writeTo(com_data);
            flushCommand();

            return dispatch_id;
        }

        /**
         * Releases the server side resources associated with a given query key
         * returned by the server.  This should be called when the ResultSet is
         * closed, or if we cancel in the middle of downloading a result.
         * <p>
         * It's very important that the server resources for a query is released.
         * <p>
         * Returns the dispatch id key for the response from the server.
         */
        synchronized int disposeResult(int result_id) throws IOException {
            int dispatch_id = nextDispatchID();
            com_data.writeInt(DISPOSE_RESULT);
            com_data.writeInt(dispatch_id);
            com_data.writeInt(result_id);
            flushCommand();

            return dispatch_id;
        }

        /**
         * Requests a part of a result of a query.  This is used to download a
         * part of a result set from the server.  The 'result_id' is generated
         * by the 'query' command.  Please note that this will generate an error
         * if the result_id is invalid or has previously been disposed.  The
         * 'row_number' refers to the row to download from.  The 'row_count'
         * refers to the number of rows to download.
         * <p>
         * Returns the dispatch id key for the response from the server.
         */
        synchronized int getResultPart(int result_id, int row_number,
                                       int row_count) throws IOException {
            int dispatch_id = nextDispatchID();
            com_data.writeInt(RESULT_SECTION);
            com_data.writeInt(dispatch_id);
            com_data.writeInt(result_id);
            com_data.writeInt(row_number);
            com_data.writeInt(row_count);
            flushCommand();

            return dispatch_id;
        }

        /**
         * Requests a part of an open StreamableObject channel.  This is used to
         * download a section of a large object, such as a Blob or a Clob.  The
         * 'streamable_object_id' is returned by the 'getIdentifier' method of the
         * StreamableObject in a ResultPart.
         * <p>
         * Returns the dispatch id key for the response from the server.
         */
        synchronized int getStreamableObjectPart(int result_id,
                                                 long streamable_object_id,
                                                 long offset, int length) throws IOException {
            int dispatch_id = nextDispatchID();
            com_data.writeInt(STREAMABLE_OBJECT_SECTION);
            com_data.writeInt(dispatch_id);
            com_data.writeInt(result_id);
            com_data.writeLong(streamable_object_id);
            com_data.writeLong(offset);
            com_data.writeInt(length);
            flushCommand();

            return dispatch_id;
        }

        /**
         * Disposes the resources associated with a streamable object on the server.
         * This would typically be called when either of the following situations
         * occured - the Blob is closed/disposed/finalized, the InputStream is
         * closes/finalized.
         * <p>
         * It's very important that the server resources for a streamable object is
         * released.
         * <p>
         * Returns the dispatch id key for the response from the server.
         */
        synchronized int disposeStreamableObject(int result_id,
                                                 long streamable_object_id) throws IOException {
            int dispatch_id = nextDispatchID();
            com_data.writeInt(DISPOSE_STREAMABLE_OBJECT);
            com_data.writeInt(dispatch_id);
            com_data.writeInt(result_id);
            com_data.writeLong(streamable_object_id);
            flushCommand();

            return dispatch_id;
        }

        /**
         * Sends close command to server.
         */
        synchronized int sendCloseCommand() throws IOException {
            int dispatch_id = nextDispatchID();
            com_data.writeInt(CLOSE);
            com_data.writeInt(dispatch_id);
            flushCommand();

            return dispatch_id;
        }


        // ---------- Server read methods ----------


        /**
         * Listens for commands from the server.  When received puts the command
         * on the dispatch list.
         */
        public void run() {

            try {
                while (!thread_closed) {

                    // Block until next command received from server.
                    byte[] buf = nextCommandFromServer(0);
                    int dispatch_id = ByteArrayUtil.getInt(buf, 0);

                    if (dispatch_id == -1) {
                        // This means a trigger or a ping or some other server side event.
                        processEvent(buf);
                    }

                    synchronized (commands_list) {
                        // Add this command to the commands list
                        commands_list.addElement(new ServerCommand(dispatch_id, buf));
                        // Notify any threads waiting on it.
                        commands_list.notifyAll();
                    }

                } // while(true)

            } catch (IOException e) {
//      System.err.println("Connection Thread closed because of IOException");
//      e.printStackTrace();
            } finally {
                // Invalidate this object when the thread finishes.
                Object old_commands_list = commands_list;
                synchronized (old_commands_list) {
                    commands_list = null;
                    old_commands_list.notifyAll();
                }
            }

        }

        /**
         * Processes a server side event.
         */
        private void processEvent(byte[] buf) throws IOException {
            int event = ByteArrayUtil.getInt(buf, 4);
            if (event == PING) {
                // Ignore ping events, they only sent by server to see if we are
                // alive.  Ping back?
            } else if (event == DATABASE_EVENT) {
                // A database event that is passed to the DatabaseCallBack...
                ByteArrayInputStream bin =
                        new ByteArrayInputStream(buf, 8, buf.length - 8);
                DataInputStream din = new DataInputStream(bin);

                int event_type = din.readInt();
                String event_msg = din.readUTF();
                database_call_back.databaseEvent(event_type, event_msg);
            }
//      else if (event == SERVER_REQUEST) {
//        // A server request that is passed to the DatabaseCallBack...
//        ByteArrayInputStream bin =
//                              new ByteArrayInputStream(buf, 8, buf.length - 8);
//        DataInputStream din = new DataInputStream(bin);
//
//        int command = din.readInt();        // Currently ignored
//        long stream_id = din.readLong();
//        int length = din.readInt();
//        database_call_back.streamableObjectRequest(stream_id, length);
//      }
            else {
                System.err.println("[RemoteDatabaseInterface] " +
                        "Received unrecognised server side event: " + event);
            }
        }

    }

    /**
     * A ByteArrayOutputStream that allows us access to the underlying byte[]
     * array.
     */
    static class MByteArrayOutputStream extends ByteArrayOutputStream {
        MByteArrayOutputStream() {
            super(256);
        }

        public byte[] getBuffer() {
            return buf;
        }

        public int size() {
            return count;
        }
    }

    /**
     * Represents the data in a command from the server.
     */
    static class ServerCommand {

        private final int dispatch_id;
        private final byte[] buf;

        ServerCommand(int dispatch_id, byte[] buf) {
            this.dispatch_id = dispatch_id;
            this.buf = buf;
        }

        public int dispatchID() {
            return dispatch_id;
        }

        public byte[] getBuf() {
            return buf;
        }

        public ByteArrayInputStream getInputStream() {
            return new ByteArrayInputStream(buf, 4, buf.length - 4);
        }

    }

}
