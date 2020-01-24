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
