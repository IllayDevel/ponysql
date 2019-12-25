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

import com.pony.database.global.ObjectTransfer;
import com.pony.database.jdbc.StreamableObjectPart;
import com.pony.database.jdbc.ProtocolConstants;
import com.pony.database.jdbc.MSQLException;
import com.pony.database.jdbc.DatabaseCallBack;
import com.pony.database.jdbc.DatabaseInterface;
import com.pony.database.jdbc.QueryResponse;
import com.pony.database.jdbc.ResultPart;
import com.pony.database.jdbc.SQLQuery;
import com.pony.debug.*;
import com.pony.util.ByteArrayUtil;

import java.sql.SQLException;
import java.io.*;

/**
 * This processes JDBC commands from a JDBC client and dispatches the commands
 * to the database.  This is a state based class.  There is a single processor
 * for each JDBC client connected.  This class is designed to be flexible
 * enough to handle packet based protocols as well as stream based
 * protocols.
 *
 * @author Tobias Downer
 */

abstract class JDBCProcessor implements ProtocolConstants {

    /**
     * The version of the server protocol.
     */
    private static final int SERVER_VERSION = 1;


    /**
     * The current state we are in.  0 indicates we haven't logged in yet.  100
     * indicates we are logged in.
     */
    private int state;

    /**
     * Number of authentications tried.
     */
    private int authentication_tries;

    /**
     * The interface to the database.
     */
    private final DatabaseInterface db_interface;

    /**
     * An object the debug information can be logged to.
     */
    private final DebugLogger debug;

    /**
     * Sets up the processor.
     */
    JDBCProcessor(DatabaseInterface db_interface, DebugLogger logger) {
        this.debug = logger;
        this.db_interface = db_interface;
        state = 0;
        authentication_tries = 0;
    }

    /**
     * The database call back method that sends database events back to the
     * client.
     */
    private final DatabaseCallBack db_call_back = new DatabaseCallBack() {
        public void databaseEvent(int event_type, String event_message) {
            try {
                // Format the call back and send the event.
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                DataOutputStream dout = new DataOutputStream(bout);
                dout.writeInt(event_type);
                dout.writeUTF(event_message);
                sendEvent(bout.toByteArray());
            } catch (IOException e) {
                debug.write(Lvl.ERROR, this, "IO Error: " + e.getMessage());
                debug.writeException(e);
            }
        }
    };

    protected static void printByteArray(byte[] array) {
        System.out.println("Length: " + array.length);
        for (int i = 0; i < array.length; ++i) {
            System.out.print(array[i]);
            System.out.print(", ");
        }
    }

    /**
     * Processes a single JDBCCommand from the client.  The command comes in as
     * a byte[] array and the response is written out as a byte[] array.  If
     * it returns 'null' then it means the connection has been closed.
     */
    byte[] processJDBCCommand(byte[] command) throws IOException {

//    printByteArray(command);

        if (state == 0) {
            // State 0 means we looking for the header...
            int magic = ByteArrayUtil.getInt(command, 0);
            // The driver version number
            int maj_ver = ByteArrayUtil.getInt(command, 4);
            int min_ver = ByteArrayUtil.getInt(command, 8);

            byte[] ack_command = new byte[4 + 1 + 4 + 1];
            // Send back an acknowledgement and the version number of the server
            ByteArrayUtil.setInt(ACKNOWLEDGEMENT, ack_command, 0);
            ack_command[4] = 1;
            ByteArrayUtil.setInt(SERVER_VERSION, ack_command, 5);
            ack_command[9] = 0;

            // Set to the next state.
            state = 4;

            // Return the acknowledgement
            return ack_command;

//      // We accept drivers equal or less than 1.00 currently.
//      if ((maj_ver == 1 && min_ver == 0) || maj_ver == 0) {
//        // Go to next state.
//        state = 4;
//        return single(ACKNOWLEDGEMENT);
//      }
//      else {
//        // Close the connection if driver invalid.
//        close();
//      }
//
//      return null;
        } else if (state == 4) {
            // State 4 means we looking for username and password...
            ByteArrayInputStream bin = new ByteArrayInputStream(command);
            DataInputStream din = new DataInputStream(bin);
            String default_schema = din.readUTF();
            String username = din.readUTF();
            String password = din.readUTF();

            try {
                boolean good = db_interface.login(default_schema, username, password,
                        db_call_back);
                if (good == false) {
                    // Close after 12 tries.
                    if (authentication_tries >= 12) {
                        close();
                    } else {
                        ++authentication_tries;
                        return single(USER_AUTHENTICATION_FAILED);
                    }
                } else {
                    state = 100;
                    return single(USER_AUTHENTICATION_PASSED);
                }
            } catch (SQLException e) {
            }
            return null;

        } else if (state == 100) {
            // Process the query
            return processQuery(command);
        } else {
            throw new Error("Illegal state: " + state);
        }

    }

    /**
     * Returns the state of the connection.  0 = not logged in yet.  1 = logged
     * in.
     */
    int getState() {
        return state;
    }

    /**
     * Convenience, returns a single 4 byte array with the given int encoded
     * into it.
     */
    private byte[] single(int val) {
        byte[] buf = new byte[4];
        ByteArrayUtil.setInt(val, buf, 0);
        return buf;
    }

    /**
     * Creates a response that represents an SQL exception failure.
     */
    private byte[] exception(int dispatch_id, SQLException e)
            throws IOException {

        int code = e.getErrorCode();
        String msg = e.getMessage();
        if (msg == null) {
            msg = "NULL exception message";
        }
        String server_msg = "";
        String stack_trace = "";

        if (e instanceof MSQLException) {
            MSQLException me = (MSQLException) e;
            server_msg = me.getServerErrorMsg();
            stack_trace = me.getServerErrorStackTrace();
        } else {
            StringWriter writer = new StringWriter();
            e.printStackTrace(new PrintWriter(writer));
            stack_trace = writer.toString();
        }

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeInt(dispatch_id);
        dout.writeInt(EXCEPTION);
        dout.writeInt(code);
        dout.writeUTF(msg);
        dout.writeUTF(stack_trace);

        return bout.toByteArray();

    }

    /**
     * Creates a response that indicates a simple success of an operation with
     * the given dispatch id.
     */
    private byte[] simpleSuccess(int dispatch_id) throws IOException {
        byte[] buf = new byte[8];
        ByteArrayUtil.setInt(dispatch_id, buf, 0);
        ByteArrayUtil.setInt(SUCCESS, buf, 4);
        return buf;
    }

    /**
     * Processes a query on the byte[] array and returns the result.
     */
    private byte[] processQuery(byte[] command) throws IOException {

        byte[] result;

        // The first int is the command.
        int ins = ByteArrayUtil.getInt(command, 0);

        // Otherwise must be a dispatch type request.
        // The second is the dispatch id.
        int dispatch_id = ByteArrayUtil.getInt(command, 4);

        if (dispatch_id == -1) {
            throw new Error("Special case dispatch id of -1 in query");
        }

        if (ins == RESULT_SECTION) {
            result = resultSection(dispatch_id, command);
        } else if (ins == QUERY) {
            result = queryCommand(dispatch_id, command);
        } else if (ins == PUSH_STREAMABLE_OBJECT_PART) {
            result = pushStreamableObjectPart(dispatch_id, command);
        } else if (ins == DISPOSE_RESULT) {
            result = disposeResult(dispatch_id, command);
        } else if (ins == STREAMABLE_OBJECT_SECTION) {
            result = streamableObjectSection(dispatch_id, command);
        } else if (ins == DISPOSE_STREAMABLE_OBJECT) {
            result = disposeStreamableObject(dispatch_id, command);
        } else if (ins == CLOSE) {
            close();
            result = null;
        } else {
            throw new Error("Command (" + ins + ") not understood.");
        }

        return result;

    }

    /**
     * Disposes of this processor.
     */
    void dispose() {
        try {
            db_interface.dispose();
        } catch (Throwable e) {
            debug.writeException(Lvl.ERROR, e);
        }
    }


    // ---------- JDBC primitive commands ----------

    /**
     * Executes a query and returns the header for the result in the response.
     * This keeps track of all result sets because sections of the result are
     * later queries via the 'RESULT_SECTION' command.
     * <p>
     * 'dispatch_id' is the number we need to respond with.
     */
    private byte[] queryCommand(int dispatch_id,
                                byte[] command) throws IOException {

        // Read the query from the command.
        ByteArrayInputStream bin =
                new ByteArrayInputStream(command, 8, command.length - 8);
        DataInputStream din = new DataInputStream(bin);
        SQLQuery query = SQLQuery.readFrom(din);

        try {
            // Do the query
            QueryResponse response = db_interface.execQuery(query);

            // Prepare the stream to output the response to,
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(bout);

            dout.writeInt(dispatch_id);
            dout.writeInt(SUCCESS);

            // The response sends the result id, the time the query took, the
            // total row count, and description of each column in the result.
            dout.writeInt(response.getResultID());
            dout.writeInt(response.getQueryTimeMillis());
            dout.writeInt(response.getRowCount());
            int col_count = response.getColumnCount();
            dout.writeInt(col_count);
            for (int i = 0; i < col_count; ++i) {
                response.getColumnDescription(i).writeTo(dout);
            }

            return bout.toByteArray();

        } catch (SQLException e) {
//      debug.writeException(e);
            return exception(dispatch_id, e);
        }

    }

    /**
     * Pushes a part of a streamable object onto the server.
     * <p>
     * 'dispatch_id' is the number we need to respond with.
     */
    private byte[] pushStreamableObjectPart(int dispatch_id,
                                            byte[] command) throws IOException {
        byte type = command[8];
        long object_id = ByteArrayUtil.getLong(command, 9);
        long object_length = ByteArrayUtil.getLong(command, 17);
        int length = ByteArrayUtil.getInt(command, 25);
        byte[] ob_buf = new byte[length];
        System.arraycopy(command, 29, ob_buf, 0, length);
        long offset = ByteArrayUtil.getLong(command, 29 + length);

        try {
            // Pass this through to the underlying database interface.
            db_interface.pushStreamableObjectPart(type, object_id, object_length,
                    ob_buf, offset, length);

            // Return operation success.
            return simpleSuccess(dispatch_id);

        } catch (SQLException e) {
            return exception(dispatch_id, e);
        }

    }

    /**
     * Responds with a part of the result set of a query made via the 'QUERY'
     * command.
     * <p>
     * 'dispatch_id' is the number we need to respond with.
     */
    private byte[] resultSection(int dispatch_id,
                                 byte[] command) throws IOException {

        int result_id = ByteArrayUtil.getInt(command, 8);
        int row_number = ByteArrayUtil.getInt(command, 12);
        int row_count = ByteArrayUtil.getInt(command, 16);

        try {
            // Get the result part...
            ResultPart block =
                    db_interface.getResultPart(result_id, row_number, row_count);

            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(bout);

            dout.writeInt(dispatch_id);
            dout.writeInt(SUCCESS);

            // Send the contents of the result set.
            // HACK - Work out column count by dividing number of entries in block
            //   by number of rows.
            int col_count = block.size() / row_count;
            dout.writeInt(col_count);
            int bsize = block.size();
            for (int index = 0; index < bsize; ++index) {
                ObjectTransfer.writeTo(dout, block.elementAt(index));
            }

            return bout.toByteArray();
        } catch (SQLException e) {
            return exception(dispatch_id, e);
        }
    }

    /**
     * Returns a section of a streamable object.
     * <p>
     * 'dispatch_id' is the number we need to respond with.
     */
    private byte[] streamableObjectSection(int dispatch_id, byte[] command)
            throws IOException {
        int result_id = ByteArrayUtil.getInt(command, 8);
        long streamable_object_id = ByteArrayUtil.getLong(command, 12);
        long offset = ByteArrayUtil.getLong(command, 20);
        int length = ByteArrayUtil.getInt(command, 28);

        try {
            StreamableObjectPart ob_part =
                    db_interface.getStreamableObjectPart(result_id, streamable_object_id,
                            offset, length);

            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(bout);

            dout.writeInt(dispatch_id);
            dout.writeInt(SUCCESS);

            byte[] buf = ob_part.getContents();
            dout.writeInt(buf.length);
            dout.write(buf, 0, buf.length);

            return bout.toByteArray();
        } catch (SQLException e) {
            return exception(dispatch_id, e);
        }

    }

    /**
     * Disposes of a streamable object.
     * <p>
     * 'dispatch_id' is the number we need to respond with.
     */
    private byte[] disposeStreamableObject(int dispatch_id, byte[] command)
            throws IOException {
        int result_id = ByteArrayUtil.getInt(command, 8);
        long streamable_object_id = ByteArrayUtil.getLong(command, 12);

        try {
            // Pass this through to the underlying database interface.
            db_interface.disposeStreamableObject(result_id, streamable_object_id);

            // Return operation success.
            return simpleSuccess(dispatch_id);

        } catch (SQLException e) {
            return exception(dispatch_id, e);
        }
    }

    /**
     * Disposes of a result set we queries via the 'QUERY' command.
     * <p>
     * 'dispatch_id' is the number we need to respond with.
     */
    private byte[] disposeResult(int dispatch_id,
                                 byte[] command) throws IOException {

        // Get the result id.
        int result_id = ByteArrayUtil.getInt(command, 8);

        try {
            // Dispose the table.
            db_interface.disposeResult(result_id);
            // Return operation success.
            return simpleSuccess(dispatch_id);
        } catch (SQLException e) {
            return exception(dispatch_id, e);
        }
    }


    // ---------- Abstract methods ----------

    /**
     * Sends an event to the client.  This is used to notify the client of
     * trigger events, etc.
     * <p>
     * SECURITY ISSUE: This is always invoked by the DatabaseDispatcher.  We
     *   have to be careful that this method isn't allowed to block.  Otherwise
     *   the DatabaseDispatcher thread will be out of operation.  Unfortunately
     *   assuring this may not be possible until Java has non-blocking IO, or we
     *   use datagrams for transmission.  I know for sure that the TCP
     *   implementation is vunrable.  If the client doesn't 'read' what we are
     *   sending then this'll block when the buffers become full.
     */
    public abstract void sendEvent(byte[] event_msg) throws IOException;

    /**
     * Closes the connection with the client.
     */
    public abstract void close() throws IOException;

    /**
     * Returns true if the connection to the client is closed.
     */
    public abstract boolean isClosed() throws IOException;

    // ---------- Finalize ----------

    public final void finalize() throws Throwable {
        super.finalize();
        try {
            dispose();
        } catch (Throwable e) { /* ignore */ }
    }

}
