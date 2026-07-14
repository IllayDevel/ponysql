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

package com.pony.util;

import java.sql.*;
import java.io.*;
import java.util.Vector;

/**
 * Utilities for parsing a ResultSet and outputing it in different forms.  The
 * forms included are straight text (mono-spaced), HTML, etc.
 *
 * @author Tobias Downer
 */

public class ResultOutputUtil {


    /**
     * Writes a break.
     *   eg. "+--------+----------+---------------+"
     */
    private static void writeBreak(int[] widths, PrintWriter out) {
        out.print('+');
        for (int width : widths) {
            int wid = width + 2;
            for (int n = 0; n < wid; ++n) {
                out.print('-');
            }
            out.print('+');
        }
        out.println();
    }

    /**
     * Writes a row of data.
     *   eg. "|1         |Greetings        |Part-54445    |"
     */
    private static void writeRow(int[] widths, String[] cols, PrintWriter out) {
        out.print('|');
        for (int i = 0; i < widths.length; ++i) {
            String str = cols[i];
            out.print(' ');
            out.print(str);
            // Write padding
            int wid = (widths[i] + 1) - str.length();
            for (int n = 0; n < wid; ++n) {
                out.print(' ');
            }
            out.print('|');
        }
        out.println();
    }

    /**
     * Formats the ResultSet as plain mono-spaced text and outputs the result to
     * the PrintWriter.
     */
    public static void formatAsText(ResultSet result_set, PrintWriter out)
            throws SQLException {
        ResultSetMetaData meta_data = result_set.getMetaData();
        // Maximum widths of each column.
        int[] max_widths = new int[meta_data.getColumnCount()];
        Vector[] data = new Vector[meta_data.getColumnCount()];
        for (int i = 0; i < data.length; ++i) {
            data[i] = new Vector();
        }
        int row_count = 0;

        for (int i = 0; i < data.length; ++i) {
            String str = meta_data.getColumnLabel(i + 1);
            max_widths[i] = Math.max(str.length(), max_widths[i]);
        }

        // Read in the data for the result set,
        while (result_set.next()) {
            for (int i = 0; i < data.length; ++i) {
                Object ob = result_set.getObject(i + 1);
                String str = "NULL";
                if (ob != null) {
                    str = ob.toString();
                }
                data[i].addElement(str);
                max_widths[i] = Math.max(str.length(), max_widths[i]);
            }
            ++row_count;
        }

        // Output the data we stored
        String[] line = new String[data.length];

        writeBreak(max_widths, out);
        for (int n = 0; n < line.length; ++n) {
            line[n] = meta_data.getColumnLabel(n + 1);
        }
        writeRow(max_widths, line, out);
        writeBreak(max_widths, out);
        for (int i = 0; i < row_count; ++i) {
            for (int n = 0; n < line.length; ++n) {
                line[n] = (String) data[n].elementAt(i);
            }
            writeRow(max_widths, line, out);
        }
        writeBreak(max_widths, out);

    }


}
