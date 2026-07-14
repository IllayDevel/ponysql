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

package com.pony.database.jdbc;

import java.io.*;

/**
 * An stream implementation of an interface to a Pony database.  This
 * is a stream based communication protocol.
 *
 * @author Tobias Downer
 */

class StreamDatabaseInterface extends RemoteDatabaseInterface {

    /**
     * The data output stream for the db protocol.
     */
    protected DataOutputStream out;

    /**
     * The data input stream for the db protocol.
     */
    protected DataInputStream in;

    private boolean closed = false;


//  /**
//   * Constructor.
//   */
//  StreamDatabaseInterface(String db_name) {
//    super(db_name);
//  }

    /**
     * Sets up the stream connection with the given input/output stream.
     */
    void setup(InputStream rawin, OutputStream rawout) throws IOException {
//    System.out.println("rawin: " + rawin);
//    System.out.println("rawout: " + rawout);
        if (rawin == null || rawout == null) {
            throw new IOException("rawin or rawin is null");
        }
        // Get the input and output and wrap around Data streams.
        in = new DataInputStream(new BufferedInputStream(rawin, 32768));
        out = new DataOutputStream(new BufferedOutputStream(rawout, 32768));
    }


    /**
     * Writes the given command to the server.  The stream protocol flushes the
     * byte array onto the stream.
     */
    void writeCommandToServer(byte[] command, int offset, int size)
            throws IOException {
        out.writeInt(size);
        out.write(command, 0, size);
        out.flush();
    }

    /**
     * Blocks until the next command is received from the server.  The stream
     * protocol waits until we receive something from the server.
     */
    byte[] nextCommandFromServer(int timeout) throws IOException {
        if (closed) {
            throw new IOException("DatabaseInterface is closed!");
        }
        try {
//      System.out.println("I'm waiting for a command: " + this);
//      new Error().printStackTrace();
            int command_length = in.readInt();
            byte[] buf = new byte[command_length];
            in.readFully(buf, 0, command_length);
            return buf;
        } catch (NullPointerException e) {
            System.out.println("Throwable generated at: " + this);
            throw e;
        }
    }

    void closeConnection() throws IOException {
//    System.out.println("Closed: " + this);
        closed = true;
        try {
            out.close();
        } catch (IOException e) {
            in.close();
            throw e;
        }
        in.close();
    }

}
