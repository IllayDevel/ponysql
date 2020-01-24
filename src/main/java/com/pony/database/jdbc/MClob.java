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

import java.sql.SQLException;
import java.sql.Clob;
import java.io.StringReader;
import java.io.Reader;

/**
 * An implementation of java.sql.Clob over a java.util.String object.
 *
 * @author Tobias Downer
 */

class MClob implements Clob {

    /**
     * The string the Clob is based on.
     */
    private final String str;

    /**
     * Constructs the Clob implementation.
     */
    public MClob(String str) {
        this.str = str;
    }

    // ---------- Implemented from Clob ----------

    public long length() throws SQLException {
        return str.length();
    }

    public String getSubString(long pos, int length) throws SQLException {
        int p = (int) (pos - 1);
        return str.substring(p, p + length);
    }

    public Reader getCharacterStream() throws SQLException {
        return new StringReader(str);
    }

    public java.io.InputStream getAsciiStream() throws SQLException {
        return new AsciiInputStream(getCharacterStream());
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
        long s = pos;
        long e = pos + length;
        if (s > Integer.MAX_VALUE || s < 0 ||
                e > Integer.MAX_VALUE || e < 0 ||
                s > e) {
            throw new java.lang.IndexOutOfBoundsException();
        }

        return new StringReader(str.substring((int) s, (int) e));
    }

    //#ENDIF

}

