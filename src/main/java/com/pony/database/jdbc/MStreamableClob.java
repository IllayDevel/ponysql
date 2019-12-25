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
import java.sql.Clob;
import java.sql.SQLException;

/**
 * A Clob that is a large object that may be streamed from the server directly
 * to this object.  A clob that is streamable is only alive for the lifetime of
 * the result set it is part of.  If the underlying result set that contains
 * this streamable clob is closed then this clob is no longer valid.
 *
 * @author Tobias Downer
 */

class MStreamableClob extends AbstractStreamableObject implements Clob {

    /**
     * Constructs the Clob.
     */
    MStreamableClob(MConnection connection, int result_set_id, byte type,
                    long streamable_object_id, long size) {
        super(connection, result_set_id, type, streamable_object_id, size);
    }

    // ---------- Implemented from Blob ----------

    public long length() throws SQLException {
        if (getType() == 4) {
            return rawSize() / 2;
        }
        return rawSize();
    }

    public String getSubString(long pos, int length) throws SQLException {
        int p = (int) (pos - 1);
        Reader reader = getCharacterStream();
        try {
            reader.skip(p);
            StringBuffer buf = new StringBuffer(length);
            for (int i = 0; i < length; ++i) {
                int c = reader.read();
                buf.append((char) c);
            }
            return new String(buf);
        } catch (IOException e) {
            e.printStackTrace(System.err);
            throw new SQLException("IO Error: " + e.getMessage());
        }
    }

    public Reader getCharacterStream() throws SQLException {
        if (getType() == 3) {
            return new AsciiReader(new StreamableObjectInputStream(rawSize()));
        } else if (getType() == 4) {
            return new BinaryToUnicodeReader(
                    new StreamableObjectInputStream(rawSize()));
        } else {
            throw new SQLException("Unknown type.");
        }
    }

    public java.io.InputStream getAsciiStream() throws SQLException {
        if (getType() == 3) {
            return new StreamableObjectInputStream(rawSize());
        } else if (getType() == 4) {
            return new AsciiInputStream(getCharacterStream());
        } else {
            throw new SQLException("Unknown type.");
        }
    }

    public long position(String searchstr, long start) throws SQLException {
        throw MSQLException.unsupported();
    }

    public long position(Clob searchstr, long start) throws SQLException {
        throw MSQLException.unsupported();
    }

    //#IFDEF(JDBC3.0)

    //---------------------------- JDBC 3.0 -----------------------------------

    public int setString(long pos, String str) throws SQLException {
        throw MSQLException.unsupported();
    }

    public int setString(long pos, String str, int offset, int len)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public java.io.OutputStream setAsciiStream(long pos) throws SQLException {
        throw MSQLException.unsupported();
    }

    public java.io.Writer setCharacterStream(long pos) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void truncate(long len) throws SQLException {
        throw MSQLException.unsupported();
    }

    //#ENDIF

//#IFDEF(JDBC4.0)

    // -------------------------- JDK 1.6 -----------------------------------

    public void free() throws SQLException {
    }

    public Reader getCharacterStream(long pos, long length) throws SQLException {
        if (getType() == 3) {
            long raw_end = pos + length;
            if (raw_end > rawSize() || raw_end < 0) {
                throw new java.lang.IndexOutOfBoundsException();
            }

            return new AsciiReader(new StreamableObjectInputStream(rawSize()));
        } else if (getType() == 4) {
            long raw_end = pos + (length * 2);
            if (raw_end > rawSize() || raw_end < 0) {
                throw new java.lang.IndexOutOfBoundsException();
            }

            return new BinaryToUnicodeReader(
                    new StreamableObjectInputStream(rawSize()));
        } else {
            throw new SQLException("Unknown type.");
        }
    }

    //#ENDIF

}
