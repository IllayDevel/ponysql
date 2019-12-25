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
import java.sql.SQLException;

import com.pony.database.global.ObjectTransfer;
import com.pony.database.global.ObjectTranslator;
import com.pony.database.global.ByteLongObject;

/**
 * Represents an SQL Query to the database.  This includes the query string
 * itself plus any data types that are part of the query.
 * <p>
 * FUTURE ENHANCEMENTS: This could do some preliminary parsing of the query
 *   string for faster translation by the database.
 *
 * @author Tobias Downer
 */

public final class SQLQuery {

    /**
     * The SQL String.  For example, "select * from Part".
     */
    private String query;

    /**
     * Set to true when this query is prepared via the prepare method.
     */
    private boolean prepared;

    /**
     * The list of all variable substitutions that are in the query.  A
     * variable substitution is set up in a prepared statement.
     */
    private Object[] parameters;
    private int parameters_index;
    private int parameter_count;


    /**
     * Empty constructor.
     */
    private SQLQuery() {
    }

    /**
     * Constructs the query.
     */
    public SQLQuery(String query) {
        this.query = query;
        parameters = new Object[8];
        parameters_index = 0;
        parameter_count = 0;
        prepared = false;
    }

    /**
     * Grows the parameters list to the given size.
     */
    private void growParametersList(int new_size) {
        // Make new list
        Object[] new_list = new Object[new_size];
        // Copy everything to new list
        System.arraycopy(parameters, 0, new_list, 0, parameters.length);
        // Set the new list.
        parameters = new_list;
    }

    /**
     * Translates the given object to a type the object can process.
     */
    private Object translateObjectType(Object ob) {
        return ObjectTranslator.translate(ob);
    }

    /**
     * Adds a variable to the query.  If the object is not a type that is
     * a database 'primitive' type (BigDecimal, ByteLongObject, Boolean,
     * Date, String) then it is serialized and the serialized form is wrapped
     * in a ByteLongObject.
     */
    public void addVar(Object ob) {
        ob = translateObjectType(ob);
        parameters[parameters_index] = ob;
        ++parameters_index;
        ++parameter_count;
        if (parameters_index >= parameters.length) {
            growParametersList(parameters_index + 8);
        }
    }

    /**
     * Sets a variable at the given index.  Grows if necessary.  If the object is
     * not a type that is a database 'primitive' type (BigDecimal,
     * ByteLongObject, Boolean, Date, String) then it is serialized and the
     * serialized form is wrapped in a ByteLongObject.
     */
    public void setVar(int i, Object ob) {
        ob = translateObjectType(ob);
        if (i >= parameters.length) {
            growParametersList(i + 8);
        }
        parameters[i] = ob;
        parameters_index = i + 1;
        parameter_count = Math.max(parameters_index, parameter_count);
    }

    /**
     * Clears all the parameters.
     */
    public void clear() {
        parameters_index = 0;
        parameter_count = 0;
        for (int i = 0; i < parameters.length; ++i) {
            parameters[i] = null;
        }
    }


    /**
     * Returns the query string.
     */
    public String getQuery() {
        return query;
    }

    /**
     * Returns the array of all objects that are to be used as substitutions
     * for '?' in the query.
     * <p>
     * NOTE: Array returned references internal Object[] here so don't change!
     */
    public Object[] getVars() {
        return parameters;
    }

    /**
     * Given a JDBC escape code of the form {keyword ... parameters ...} this
     * will return the most optimal Pony SQL query for the code.
     */
    private String escapeJDBCSubstitution(String jdbc_code)
            throws SQLException {
        String code = jdbc_code.substring(1, jdbc_code.length() - 1);
        int kp_delim = code.indexOf(' ');
        if (kp_delim != -1) {
            String keyword = code.substring(0, kp_delim);
            String body = code.substring(kp_delim).trim();

            if (keyword.equals("d")) {   // Process a date
                return "DATE " + body;
            }
            if (keyword.equals("t")) {   // Process a time
                return "TIME " + body;
            }
            if (keyword.equals("ts")) {  // Process a timestamp
                return "TIMESTAMP " + body;
            }
            if (keyword.equals("fn")) {  // A function
                return body;
            }
            if (keyword.equals("call") || keyword.equals("?=")) {
                throw new MSQLException("Stored procedures not supported.");
            }
            if (keyword.equals("oj")) {  // Outer join
                return body;
            }

            throw new MSQLException("Do not understand JDBC substitution keyword '" +
                    keyword + "' of " + jdbc_code);
        } else {
            throw new MSQLException("Malformed JDBC escape code: " + jdbc_code);
        }

    }

    /**
     * Performs any JDBC escape processing on the query.  For example, the
     * code {d 'yyyy-mm-dd'} is converted to 'DATE 'yyyy-mm-dd'.
     */
    private void doEscapeSubstitutions() throws SQLException {
        // This is a fast but primitive parser that scans the SQL string and
        // substitutes any {[code] ... } type escape sequences to the Pony
        // equivalent.  This will not make substitutions of anything inside a
        // quoted area of the query.

        // Exit early if no sign of an escape code
        if (query.indexOf('{') == -1) {
            return;
        }

        StringBuffer buf = new StringBuffer();
        StringBuffer jdbc_escape = null;

        int i = 0;
        int sz = query.length();
        int state = 0;
        boolean ignore_next = false;

        while (i < sz) {
            char c = query.charAt(i);

            if (state == 0) {  // If currently processing SQL code
                if (c == '\'' || c == '\"') {
                    state = c;     // Set state to quote
                } else if (c == '{') {
                    jdbc_escape = new StringBuffer();
                    state = '}';
                }
            } else if (state != 0) {  // If currently inside a quote or escape
                if (!ignore_next) {
                    if (c == '\\') {
                        ignore_next = true;
                    } else {
                        // If at the end of a quoted area
                        if (c == (char) state) {
                            state = 0;
                            if (c == '}') {
                                jdbc_escape.append('}');
                                buf.append(escapeJDBCSubstitution(new String(jdbc_escape)));
                                jdbc_escape = null;
                                c = ' ';
                            }
                        }
                    }
                } else {
                    ignore_next = false;
                }
            }

            if (state != '}') {
                // Copy the character
                buf.append(c);
            } else {
                jdbc_escape.append(c);
            }

            ++i;
        }

        if (state == '}') {
            throw new SQLException("Unterminated JDBC escape code in query: " +
                    new String(jdbc_escape));
        }

        query = new String(buf);
    }

    /**
     * Prepares the query by parsing the query string and performing any updates
     * that are required before being passed down to the lower layers of the
     * database engine for processing.  For example, JDBC escape code processing.
     */
    public void prepare(boolean do_escape_processing) throws SQLException {
        if (do_escape_processing) {
            doEscapeSubstitutions();
        }
        prepared = true;
    }

    /**
     * Returns true if this query is equal to another.
     */
    public boolean equals(Object ob) {
        SQLQuery q2 = (SQLQuery) ob;
        // NOTE: This could do syntax analysis on the query string to determine
        //   if it's the same or not.
        if (query.equals(q2.query)) {
            if (parameter_count == q2.parameter_count) {
                for (int i = 0; i < parameter_count; ++i) {
                    if (parameters[i] != q2.parameters[i]) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Creates an exact copy of this object.
     */
    public SQLQuery copy() {
        SQLQuery q = new SQLQuery();
        q.query = query;
        q.parameters = (Object[]) parameters.clone();
        q.parameters_index = parameters_index;
        q.parameter_count = parameter_count;
        q.prepared = prepared;
        return q;
    }

    /**
     * Outputs the query as text (for debugging)
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[ Query:\n[ ");
        buf.append(getQuery());
        buf.append(" ]\n");
        if (parameter_count > 0) {
            buf.append("\nParams:\n[ ");
            for (int i = 0; i < parameter_count; ++i) {
                Object ob = parameters[i];
                if (ob == null) {
                    buf.append("NULL");
                } else {
                    buf.append(parameters[i].toString());
                }
                buf.append(", ");
            }
            buf.append(" ]");
        }
        buf.append("\n]");
        return new String(buf);
    }

    // ---------- Stream transfer methods ----------

    /**
     * Writes the SQL query to the data output stream.
     */
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeUTF(query);
        out.writeInt(parameter_count);
        for (int i = 0; i < parameter_count; ++i) {
            ObjectTransfer.writeTo(out, parameters[i]);
        }
    }

    /**
     * Reads an SQLQuery object from the data input stream.
     */
    public static SQLQuery readFrom(DataInputStream in) throws IOException {
        String query_string = in.readUTF();
        SQLQuery query = new SQLQuery(query_string);
        int arg_length = in.readInt();
        for (int i = 0; i < arg_length; ++i) {
            query.addVar(ObjectTransfer.readFrom(in));
        }
        return query;
    }

    /**
     * Serializes an SQLQuery object to a ByteLongObject.
     */
    public ByteLongObject serializeToBlob() {

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bout);
        try {
            writeTo(out);
            out.flush();
            return new ByteLongObject(bout.toByteArray());
        } catch (IOException e) {
            throw new Error("IO Error: " + e.getMessage());
        }
    }

    /**
     * Deserializes an SQLQuery object from a ByteLongObject.
     */
    public static SQLQuery deserializeFromBlob(ByteLongObject ob) {
        DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(ob.getByteArray()));
        try {
            return readFrom(in);
        } catch (IOException e) {
            throw new Error("IO Error: " + e.getMessage());
        }
    }


}
