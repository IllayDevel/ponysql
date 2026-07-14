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
