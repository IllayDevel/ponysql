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

package com.pony.database;

import java.io.PrintStream;

/**
 * A helper class for the 'Table.dumpTo' method.  This provides variables
 * static methods for formating the contents of a table and outputting it to
 * an output stream.
 *
 * @author Tobias Downer
 */
class DumpHelper {

    /**
     * Dumps the contents of a table to the given output stream.  It uses a
     * very simple method to format the text.
     */
    static void dump(Table table, PrintStream out) {

        int col_count = table.getColumnCount();

//    if (table instanceof DataTable) {
//      DataTable data_tab = (DataTable) table;
//      out.println("Total Hits: " + data_tab.getTotalHits());
//      out.println("File Hits:  " + data_tab.getFileHits());
//      out.println("Cache Hits: " + data_tab.getCacheHits());
//      out.println();
//    }

        out.println("Table row count: " + table.getRowCount());
        out.print("      ");  // 6 spaces

        // First output the column header.
        for (int i = 0; i < col_count; ++i) {
            out.print(table.getResolvedVariable(i).toString());
            if (i < col_count - 1) {
                out.print(", ");
            }
        }
        out.println();

        // Print out the contents of each row
        int row_num = 0;
        RowEnumeration r_enum = table.rowEnumeration();
        while (r_enum.hasMoreRows() && row_num < 250) {
            // Print the row number
            String num = Integer.toString(row_num);
            int space_gap = 4 - num.length();
            for (int i = 0; i < space_gap; ++i) {
                out.print(' ');
            }
            out.print(num);
            out.print(": ");

            // Print each cell in the row
            int row_index = r_enum.nextRowIndex();
            for (int col_index = 0; col_index < col_count; ++col_index) {
                TObject cell = table.getCellContents(col_index, row_index);
                out.print(cell.toString());
                if (col_index < col_count - 1) {
                    out.print(", ");
                }
            }
            out.println();

            ++row_num;
        }
        out.println("Finished: " + row_num + "/" + table.getRowCount());

    }

}
