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

package com.pony.database.jdbc;

import java.sql.*;
import java.io.*;

import com.pony.database.global.ByteLongObject;

/**
 * An implementation of an sql.Blob object.  This implementation keeps the
 * entire Blob in memory.
 * <p>
 * <strong>NOTE:</strong> java.sql.Blob is only available in JDBC 2.0
 *
 * @author Tobias Downer
 */

class MBlob implements Blob {

    /**
     * The ByteLongObject that is a container for the data in this blob.
     */
    private final ByteLongObject blob;

    /**
     * Constructs the blob.
     */
    MBlob(ByteLongObject blob) {
        this.blob = blob;
    }

    // ---------- Implemented from Blob ----------

    public long length() throws SQLException {
        return blob.length();
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        // First byte is at position 1 according to JDBC Spec.
        --pos;
        if (pos < 0 || pos + length > length()) {
            throw new SQLException("Out of bounds.");
        }

        byte[] buf = new byte[length];
        System.arraycopy(blob.getByteArray(), (int) pos, buf, 0, length);
        return buf;
    }

    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(blob.getByteArray(), 0, (int) length());
    }

    public long position(byte[] pattern, long start) throws SQLException {
        byte[] buf = blob.getByteArray();
        int len = (int) length();
        int max = ((int) length()) - pattern.length;

        int i = (int) (start - 1);
        while (true) {
            // Look for first byte...
            while (i <= max && buf[i] != pattern[0]) {
                ++i;
            }
            // Reached end so exit..
            if (i > max) {
                return -1;
            }

            // Found first character, so look for the rest...
            int search_from = i;
            int found_index = 1;
            while (found_index < pattern.length &&
                    buf[search_from] == pattern[found_index]) {
                ++search_from;
                ++found_index;
            }

            ++i;
            if (found_index >= pattern.length) {
                return i;
            }

        }

    }

    public long position(Blob pattern, long start) throws SQLException {
        byte[] buf;
        // Optimize if MBlob,
        if (pattern instanceof MBlob) {
            buf = ((MBlob) pattern).blob.getByteArray();
        } else {
            buf = pattern.getBytes(0, (int) pattern.length());
        }
        return position(buf, start);
    }

    //#IFDEF(JDBC3.0)

    // -------------------------- JDBC 3.0 -----------------------------------

    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw new SQLException("BLOB updating is not supported");
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len)
            throws SQLException {
        throw new SQLException("BLOB updating is not supported");
    }

    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLException("BLOB updating is not supported");
    }

    public void truncate(long len) throws SQLException {
        throw new SQLException("BLOB updating is not supported");
    }

    //#ENDIF

    //#IFDEF(JDBC4.0)

    // -------------------------- JDK 1.6 -----------------------------------

    public void free() throws SQLException {
    }

    public InputStream getBinaryStream(long pos, long length)
            throws SQLException {
        long s = pos;
        long e = pos + length;
        if (s > Integer.MAX_VALUE || s < 0 ||
                e > Integer.MAX_VALUE || e < 0 ||
                s > e) {
            throw new java.lang.IndexOutOfBoundsException();
        }

        return new ByteArrayInputStream(blob.getByteArray(),
                (int) pos, (int) length);
    }

    //#ENDIF

}
