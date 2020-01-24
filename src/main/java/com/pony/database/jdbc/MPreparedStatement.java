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

import com.pony.database.global.ByteLongObject;
import com.pony.database.global.CastHelper;
import com.pony.database.global.ObjectTranslator;
import com.pony.database.global.StreamableObject;
import com.pony.database.global.StringObject;
import com.pony.util.BigNumber;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;

/**
 * An implementation of a JDBC prepared statement.
 *
 * Multi-threaded issue:  This class is not designed to be multi-thread
 *  safe.  A PreparedStatement should not be accessed by concurrent threads.
 *
 * @author Tobias Downer
 */

class MPreparedStatement extends MStatement implements PreparedStatement {

    /**
     * The SQLQuery object constructed for this statement.
     */
    private SQLQuery statement;


    /**
     * Constructs the PreparedStatement.
     */
    MPreparedStatement(MConnection connection, String sql) {
        super(connection);
        statement = new SQLQuery(sql);
    }

    // ---------- Utility ----------

    /**
     * Converts the given Object to the given SQL type object.
     */
    Object convertToType(Object ob, int sqlType) throws SQLException {
        // We use CastHelper to convert to the given SQL type.
        return CastHelper.castObjectToSQLType(ob, sqlType, -1, -1,
                "requested type");
    }

    /**
     * Converts a Java Object using the JDBC type conversion rules.  For example,
     * java.lang.Double is converted to a NUMERIC type
     * (com.pony.util.BigNumber).
     */
    Object castToPonyObject(Object ob) throws SQLException {
        if (ob == null) {
            return ob;
        }
        if (ob instanceof String) {
            return StringObject.fromString((String) ob);
        }
        if (ob instanceof Boolean) {
            return ob;
        }
        if (ob instanceof Number) {
            Number n = (Number) ob;
            if (ob instanceof Byte ||
                    ob instanceof Short ||
                    ob instanceof Integer) {
                return BigNumber.fromInt(n.intValue());
            } else if (ob instanceof Long) {
                return BigNumber.fromLong(n.longValue());
            } else if (ob instanceof Float) {
                return BigNumber.fromFloat(n.floatValue());
            } else if (ob instanceof Double) {
                return BigNumber.fromDouble(n.doubleValue());
            } else {
                return BigNumber.fromString(n.toString());
            }
        }
        if (ob instanceof byte[]) {
            return new ByteLongObject((byte[]) ob);
        }
        try {
            return ObjectTranslator.translate(ob);
        } catch (Throwable e) {
            // Hacky - we need for ObjectTranslator to throw a better exception
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * Given an InputStream and a length of bytes to read from the stream, this
     * method will insert a correct type of parameter into the statement to handle
     * this size of object.  If the object is too large it will mark it as a
     * streamable object.
     *
     * @param parameterIndex 1 for first parameter, 2 for second, etc.
     * @param x the input stream containing the binary data.
     * @param length the number of bytes to read.
     * @param type 2 = binary, 3 = 1 byte char, 4 = 2 byte unicode.
     */
    private void setVariableLengthStream(int parameterIndex,
                                         InputStream x, int length, byte type) throws IOException {
        // If we are going to transfer more than 8K bytes then the object becomes
        // a streamable object
        if (length > 8 * 1024) {
            int p_ind = parameterIndex - 1;
            // Generate a new StreamableObject and for this InputStream and store it
            // in the hold.
            StreamableObject s_object = createStreamableObject(x, length, type);
            // Put the streamable object in the statement variable list.
            statement.setVar(p_ind, s_object);
        } else {
            // If binary stream,
            if (type == 2) {
                // Less than 8K bytes so we transfer the object as a standard
                // ByteLongObject.
                ByteLongObject b = new ByteLongObject(x, length);
                statement.setVar(parameterIndex - 1, b);
            }
            // If ascii stream
            else if (type == 3) {
                // Convert to a String
                StringBuffer buf = new StringBuffer();
                for (int i = 0; i < length; ++i) {
                    int v = x.read();
                    if (v == -1) {
                        throw new IOException("Premature EOF reached.");
                    }
                    buf.append((char) v);
                }
                statement.setVar(parameterIndex - 1,
                        StringObject.fromString(new String(buf)));
            }
            // If unicode stream
            else if (type == 4) {
                // Convert to a String
                StringBuffer buf = new StringBuffer();
                int half_len = length / 2;
                for (int i = 0; i < half_len; ++i) {
                    int v1 = x.read();
                    int v2 = x.read();
                    if (v1 == -1 || v2 == -1) {
                        throw new IOException("Premature EOF reached.");
                    }
                    buf.append((char) ((v1 << 8) + v2));
                }
                statement.setVar(parameterIndex - 1,
                        StringObject.fromString(new String(buf)));
            } else {
                throw new RuntimeException("Do not understand type.");
            }
        }
    }

    // ---------- Overridden from MStatement ----------

    public void close() throws SQLException {
        super.close();
        statement = null;
    }


    // ---------- Implemented from PreparedStatement ----------

    public ResultSet executeQuery() throws SQLException {
        return executeQuery(statement);
    }

    public int executeUpdate() throws SQLException {
        MResultSet result_set = executeQuery(statement);
        return result_set.intValue();
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        statement.setVar(parameterIndex - 1, null);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        statement.setVar(parameterIndex - 1, x);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        setLong(parameterIndex, x);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        setLong(parameterIndex, x);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        setLong(parameterIndex, x);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        statement.setVar(parameterIndex - 1, BigNumber.fromLong(x));
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        statement.setVar(parameterIndex - 1, BigNumber.fromFloat(x));
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        statement.setVar(parameterIndex - 1, BigNumber.fromDouble(x));
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x)
            throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NUMERIC);
        } else {
            statement.setVar(parameterIndex - 1, BigNumber.fromBigDecimal(x));
        }
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.VARCHAR);
        } else {
            // If the string is less than 4K characters then transfer as a regular
            // string, otherwise treat the string as a large object.
            if (x.length() <= 4 * 1024) {
                statement.setVar(parameterIndex - 1, StringObject.fromString(x));
            } else {
                setCharacterStream(parameterIndex, new StringReader(x), x.length());
            }
        }
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.BINARY);
        } else {
            // If the byte array is small then transfer as a regular ByteLongObject
            if (x.length <= 8 * 1024) {
                ByteLongObject b = new ByteLongObject(x);
                statement.setVar(parameterIndex - 1, b);
            } else {
                // Otherwise wrap around a ByteArrayInputStream and treat as a large
                // object.
                setBinaryStream(parameterIndex, new ByteArrayInputStream(x), x.length);
            }
        }
    }

    // JDBC Extension ...  Use java.util.Date as parameter
    public void extSetDate(int parameterIndex, java.util.Date x)
            throws SQLException {
        statement.setVar(parameterIndex - 1, x);
    }


    public void setDate(int parameterIndex, java.sql.Date x)
            throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.DATE);
        } else {
            extSetDate(parameterIndex, new java.util.Date(x.getTime()));
        }
    }

    public void setTime(int parameterIndex, java.sql.Time x)
            throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIME);
        } else {
            extSetDate(parameterIndex, new java.util.Date(x.getTime()));
        }
    }

    /**
     * True if the timestamp value includes nanoseconds, which is the case
     * starting with Java 1.4.0
     */
    private static final boolean TIMESTAMP_VALUE_INCLUDES_NANOS =
            (new java.sql.Timestamp(5).getTime() == 5);

    public void setTimestamp(int parameterIndex, java.sql.Timestamp x)
            throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.TIMESTAMP);
        } else {
            long time = x.getTime();
            if (!TIMESTAMP_VALUE_INCLUDES_NANOS) {
                time += (x.getNanos() / 1000000);
            }
            extSetDate(parameterIndex, new java.util.Date(time));
        }
    }

    public void setAsciiStream(int parameterIndex, java.io.InputStream x,
                               int length) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.LONGVARCHAR);
        } else {
            try {
                // Process a potentially large object.
                setVariableLengthStream(parameterIndex, x, length, (byte) 3);
            } catch (IOException e) {
                throw new SQLException("IOException reading input stream: " +
                        e.getMessage());
            }
//      // Fudge implementation since we fudged the result set version of this.
//      // In an ideal world we'd open up a stream with the server and download
//      // the information without having to collect all the data to transfer it.
//      try {
//        StringBuffer buf = new StringBuffer();
//        int i = 0;
//        while (i < length) {
//          int c = x.read();
//          if (c == -1) {
//            throw new IOException(
//                    "End of stream reached before length reached.");
//          }
//          buf.append((char) c);
//          ++i;
//        }
//        setString(parameterIndex, new String(buf));
//      }
//      catch (IOException e) {
//        e.printStackTrace();
//        throw new SQLException("IO Error: " + e.getMessage());
//      }
        }
    }

    /**
     * @deprecated
     */
    public void setUnicodeStream(int parameterIndex, java.io.InputStream x,
                                 int length) throws SQLException {
        throw new SQLException("Deprecated method not supported");
    }

    public void setBinaryStream(int parameterIndex, java.io.InputStream x,
                                int length) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.BINARY);
        } else {
            try {
                // Process a potentially large object.
                setVariableLengthStream(parameterIndex, x, length, (byte) 2);
            } catch (IOException e) {
                throw new SQLException("IOException reading input stream: " +
                        e.getMessage());
            }
        }
    }

    public void clearParameters() throws SQLException {
        statement.clear();
    }

    //----------------------------------------------------------------------
    // Advanced features:

    public void setObject(int parameterIndex, Object x, int targetSqlType,
                          int scale) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
        } else {
            x = convertToType(x, targetSqlType);
            if (x instanceof BigDecimal) {
                x = ((BigDecimal) x).setScale(scale, BigDecimal.ROUND_HALF_UP);
            }
            statement.setVar(parameterIndex - 1, x);
        }
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException {
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
        } else {
            statement.setVar(parameterIndex - 1, convertToType(x, targetSqlType));
        }
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        statement.setVar(parameterIndex - 1, castToPonyObject(x));
    }

    public boolean execute() throws SQLException {
        MResultSet result_set = executeQuery(statement);
        return !result_set.isUpdate();
    }

//#IFDEF(JDBC2.0)

    //--------------------------JDBC 2.0-----------------------------

    public void addBatch() throws SQLException {
        addBatch(statement.copy());
    }

    public void setCharacterStream(int parameterIndex,
                                   java.io.Reader reader,
                                   int length) throws SQLException {
        if (reader == null) {
            setNull(parameterIndex, Types.LONGVARCHAR);
        } else {
            try {
                // Process as a potentially large object.
                setVariableLengthStream(parameterIndex,
                        new UnicodeToBinaryStream(reader), length * 2, (byte) 4);
            } catch (IOException e) {
                throw new SQLException("IOException reading input stream: " +
                        e.getMessage());
            }
//      // NOTE: The whole stream is read into a String and the 'setString' method
//      //   is called.  This is inappropriate for long streams but probably
//      //   won't be an issue any time in the future.
//      StringBuffer buf = new StringBuffer();
//      final int BUF_LENGTH = 1024;
//      char[] char_buf = new char[BUF_LENGTH];
//      try {
//        while (length > 0) {
//          int read = reader.read(char_buf, 0, Math.min(BUF_LENGTH, length));
//          if (read > 0) {
//            buf.append(char_buf, 0, read);
//            length = length - read;
//          }
//          else {
//            throw new SQLException("Premature end of Reader reached.");
//          }
//        }
//      }
//      catch (IOException e) {
//        throw new SQLException("IOError: " + e.getMessage());
//      }
//      setString(parameterIndex, new String(buf));
        }
    }

    public void setRef(int i, Ref x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public void setBlob(int i, Blob x) throws SQLException {
        if (x == null) {
            setNull(i, Types.BLOB);
        } else {
            // BLOBs are handled the same as a binary stream.  If the length of the
            // blob exceeds a certain threshold the object is treated as a large
            // object and transferred to the server in separate chunks.
            long len = x.length();
            if (len >= 32768L * 65536L) {
                throw new SQLException("BLOB > 2 gigabytes is too large.");
            }
            setBinaryStream(i, x.getBinaryStream(), (int) len);
        }
    }

    public void setClob(int i, Clob x) throws SQLException {
        if (x == null) {
            setNull(i, Types.CLOB);
        } else {
            // CLOBs are handled the same as a character stream.  If the length of the
            // clob exceeds a certain threshold the object is treated as a large
            // object and transferred to the server in separate chunks.
            long len = x.length();
            if (len >= 16384L * 65536L) {
                throw new SQLException("CLOB > 1 billion characters is too large.");
            }
            setCharacterStream(i, x.getCharacterStream(), (int) len);
        }
    }

    public void setArray(int i, Array x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        // TODO....
        throw MSQLException.unsupported();
    }

    public void setDate(int parameterIndex, java.sql.Date x, Calendar cal)
            throws SQLException {
        // Kludge...
        setDate(parameterIndex, x);
    }

    public void setTime(int parameterIndex, java.sql.Time x, Calendar cal)
            throws SQLException {
        // Kludge...
        setTime(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, java.sql.Timestamp x,
                             Calendar cal) throws SQLException {
        // Kludge...
        setTimestamp(parameterIndex, x);
    }

    public void setNull(int paramIndex, int sqlType, String typeName)
            throws SQLException {
        // Kludge again...
        setNull(paramIndex, sqlType);
    }

//#ENDIF

//#IFDEF(JDBC3.0)

    // ---------- JDBC 3.0 ----------

    public void setURL(int parameterIndex, java.net.URL x) throws SQLException {
        throw MSQLException.unsupported();
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw MSQLException.unsupported();
    }

//#ENDIF

    /**
     * For diagnostics.
     */
    public String toString() {
        return statement.toString();
    }

}
