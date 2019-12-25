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

import java.sql.SQLException;
import java.io.*;

/**
 * SQLException used by the Pony database engine.
 *
 * @author Tobias Downer
 */

public class MSQLException extends SQLException {

    private String server_error_msg;
    private String server_stack_trace;


    public MSQLException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public MSQLException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public MSQLException(String reason) {
        super(reason);
    }

    public MSQLException() {
        super();
    }

    /**
     * MSQL Specific.  This stores the reason, the server exception message, and
     * the server stack trace.
     */
    public MSQLException(String reason, String server_error_msg, int vendor_code,
                         Throwable server_error) {
        super(reason, null, vendor_code);

        this.server_error_msg = server_error_msg;
        if (server_error != null) {
            StringWriter writer = new StringWriter();
            server_error.printStackTrace(new PrintWriter(writer));
            this.server_stack_trace = writer.toString();
        } else {
            this.server_stack_trace = "<< NO SERVER STACK TRACE >>";
        }
    }

    /**
     * MSQL Specific.  This stores the reason, the server exception message, and
     * the server stack trace as a string.
     */
    public MSQLException(String reason, String server_error_msg, int vendor_code,
                         String server_error_trace) {
        super(reason, null, vendor_code);

        this.server_error_msg = server_error_msg;
        this.server_stack_trace = server_error_trace;
    }

    /**
     * Returns the error message that generated this exception.
     */
    public String getServerErrorMsg() {
        return server_error_msg;
    }

    /**
     * Returns the server side stack trace for this error.
     */
    public String getServerErrorStackTrace() {
        return server_stack_trace;
    }

    /**
     * Overwrites the print stack trace information with some more detailed
     * information about the error.
     */
    public void printStackTrace() {
        printStackTrace(System.err);
    }

    /**
     * Overwrites the print stack trace information with some more detailed
     * information about the error.
     */
    public void printStackTrace(PrintStream s) {
        synchronized (s) {
            super.printStackTrace(s);
            if (server_stack_trace != null) {
                s.print("CAUSE: ");
                s.println(server_stack_trace);
            }
        }
    }

    /**
     * Overwrites the print stack trace information with some more detailed
     * information about the error.
     */
    public void printStackTrace(PrintWriter s) {
        synchronized (s) {
            super.printStackTrace(s);
            if (server_stack_trace != null) {
                s.print("CAUSE: ");
                s.println(server_stack_trace);
            }
        }
    }

    /**
     * Returns an SQLException that is used for all unsupported features of the
     * JDBC driver.
     */
    public static SQLException unsupported() {
        return new MSQLException("Not Supported");
    }

//#IFDEF(JDBC4.0)

    // -------------------------- JDK 1.6 -----------------------------------

    /**
     * Generates the feature not supported exception.
     */
    public static SQLException unsupported16() {
        return new java.sql.SQLFeatureNotSupportedException();
    }

//#ENDIF

}
