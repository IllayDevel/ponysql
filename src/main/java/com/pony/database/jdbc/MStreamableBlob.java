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
import java.sql.Blob;
import java.sql.SQLException;

/**
 * A Blob that is a large object that may be streamed from the server directly
 * to this object.  A blob that is streamable is only alive for the lifetime of
 * the result set it is part of.  If the underlying result set that contains
 * this streamable blob is closed then this blob is no longer valid.
 *
 * @author Tobias Downer
 */

class MStreamableBlob extends AbstractStreamableObject implements Blob {

    /**
     * Constructs the blob.
     */
    MStreamableBlob(MConnection connection, int result_set_id, byte type,
                    long streamable_object_id, long size) {
        super(connection, result_set_id, type, streamable_object_id, size);
    }

    // ---------- Implemented from Blob ----------

    public long length() throws SQLException {
        return rawSize();
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        // First byte is at position 1 according to JDBC Spec.
        --pos;
        if (pos < 0 || pos + length > length()) {
            throw new SQLException("Out of bounds.");
        }

        // The buffer we are reading into
        byte[] buf = new byte[length];
        InputStream i_stream = getBinaryStream();
        try {
            i_stream.skip(pos);
            for (int i = 0; i < length; ++i) {
                buf[i] = (byte) i_stream.read();
            }
        } catch (IOException e) {
            e.printStackTrace(System.err);
            throw new SQLException("IO Error: " + e.getMessage());
        }

        return buf;
    }

    public InputStream getBinaryStream() throws SQLException {
        return new StreamableObjectInputStream(rawSize());
    }

    public long position(byte[] pattern, long start) throws SQLException {
        throw MSQLException.unsupported();
    }

    public long position(Blob pattern, long start) throws SQLException {
        throw MSQLException.unsupported();
    }

    //#IFDEF(JDBC3.0)

    // -------------------------- JDBC 3.0 -----------------------------------

    public int setBytes(long pos, byte[] bytes) throws SQLException {

        throw MSQLException.unsupported();
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len)
            throws SQLException {
        throw MSQLException.unsupported();
    }

    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
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

    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        long end = pos + length;
        if (end > rawSize() || end < 0) {
            throw new java.lang.IndexOutOfBoundsException();
        }
        InputStream is = new StreamableObjectInputStream(end);
        try {
            is.skip(pos);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        return is;
    }

    //#ENDIF

}

