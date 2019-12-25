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

import java.sql.*;

/**
 * Wraps a Connection and provides Pony specific extensions that are
 * outside the JDBC specification.
 * <p>
 * Example,
 * <pre>
 * Connection connection = java.sql.DriverManager.getConnection( .... );
 * PonyConnection pony_connection = new PonyConnection(connection);
 * // 'pony_connection' is used for pony specific comms.
 * </pre>
 *
 * @author Tobias Downer
 */

public final class PonyConnection {

    /**
     * The wrapped MConnection.
     */
    private MConnection connection;

    /**
     * Constructs the Pony specific extension access object.
     */
    public PonyConnection(Connection connection) {
        if (connection instanceof MConnection) {
            this.connection = (MConnection) connection;
        } else {
            throw new Error("Can only wrap a Pony Database JDBC connection.");
        }
    }

    /**
     * This method can be used to disable strict get object in ResultSet.  If
     * strict get object is disabled then the 'getObject' method will return the
     * raw data type that the engine uses to represent the respective data
     * item.  If it is enabled the 'getObject' method returns the correct type
     * as specified by the JDBC spec.
     * <p>
     * Strict get is enabled by default.
     */
    public void setStrictGetObject(boolean status) {
        connection.setStrictGetObject(status);
    }

    /**
     * This method is used to enable verbose column names in ResultSetMetaData.
     * If verbose column names is enabled the getColumnName method returns
     * a string which includes the schema and table name.  This property is
     * disabled by default and provided only for compatibility with older
     * Pony applications.
     */
    public void setVerboseColumnNames(boolean status) {
        connection.setVerboseColumnNames(status);
    }

    /**
     * Registers a TriggerListener to listen for any triggers that are fired
     * with the given name.  A TriggerListener may be registered to listen for
     * multiple database triggers.
     * <p>
     * NOTE: All trigger events are fired on a dedicated trigger thread.  All
     *   triggers are fired from this thread in sequence.
     *
     * @param trigger_name the name of the database trigger to listen for.
     * @param trigger_listener the listener to be notified when the trigger
     *     event occurs.
     */
    public void addTriggerListener(String trigger_name,
                                   TriggerListener trigger_listener) {
        connection.addTriggerListener(trigger_name, trigger_listener);
    }

    /**
     * Removes a TriggerListener that is listening for triggers with the given
     * name.
     *
     * @param trigger_name the name of the database trigger to stop listening
     *     for.
     * @param trigger_listener the listener to stop being notified of trigger
     *     events for this trigger name.
     */
    public void removeTriggerListener(String trigger_name,
                                      TriggerListener trigger_listener) {
        connection.removeTriggerListener(trigger_name, trigger_listener);
    }


    // ---------- Static methods ----------

    /**
     * Given a string, this will use escape codes to convert the Java string into
     * a Pony SQL string that can be parsed correctly by the database.
     * For example;<p>
     * <pre>
     *   String user_input = [some untrusted string]
     *   Statement statement = connection.createStatement();
     *   ResultSet result = statement.executeQuery(
     *         "SELECT number FROM Part WHERE number = " +
     *         PonyConnection.quote(user_input));
     * </pre>
     * If the user supplies the string "Gr's\nut\'", this method will generate
     * the SQL query string;<p>
     * <pre>
     *   SELECT number FROM Part WHERE number = 'Gr\'s\\nut\\\''
     * </pre>
     * This is used for generating secure dynamic SQL commands.  It is
     * particularly important if the quoted strings are coming from an untrusted
     * source.
     * <p>
     * This security precaution is not necessary if using PreparedStatement to
     * form the SQL parameters.
     */
    public static String quote(String java_string) {
        StringBuffer buf = new StringBuffer();
        int str_len = java_string.length();
        for (int i = 0; i < str_len; ++i) {
            char c = java_string.charAt(i);
            if (c == '\'' || c == '\\') {
                buf.append('\\');
            }
            if (c == '\n') {
                buf.append("\\n");
            } else if (c == '\r') {
                buf.append("\\r");
            } else if (c == '\t') {
                buf.append("\\t");
            } else {
                buf.append(c);
            }
        }
        return new String(buf);
    }

}
