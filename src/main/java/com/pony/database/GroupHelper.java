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

package com.pony.database;

/**
 * This is a static class that provides the functionality for seperating a
 * table into distinct groups.  This class is used in the
 * 'Table.group(String[] columns)' method.
 * <p>
 * @author Tobias Downer
 * @deprecated don't use this anymore
 */

final class GroupHelper {

//  /**
//   * The sorted table we are grouping.
//   */
//  private Table table;
//
//  /**
//   * The table enumerator.
//   */
//  private RowEnumeration row_enum;
//
//  /**
//   * The index of the last column we are grouping on.
//   */
//  private int last_col_index;
//
//  /**
//   * The last row index.
//   */
//  private int last_row_index;
//
//  /**
//   * The columns we are grouping over.
//   */
//  private int[] columns;


    /**
     * Constructs the helper.
     */
    GroupHelper(Table table, String[] col_names) {

        throw new Error("Not used anymore");

//    // Optimisation, pre-resolve the columns into indices
//    columns = new int[col_names.length];
//    for (int i = 0; i < columns.length; ++i) {
//      int ci = table.findFieldName(col_names[i]);
//      if (ci == -1) {
//        throw new Error("Unknown field name in group ( " +
//                        col_names[i] + " )");
//      }
//      columns[i] = ci;
//    }
//
//    // Sort the tables by the column groups.
//    for (int i = col_names.length - 1; i >= 0; --i) {
////      table = table.orderByColumn(col_names[i]);
//      table = table.orderByColumn(columns[i], true);
//    }
//    this.table = table;
//
//    row_enum = table.rowEnumeration();
//    if (row_enum.hasMoreRows()) {
//      last_row_index = row_enum.nextRowIndex();
//    }
//    else {
//      last_row_index = -1;
//    }

    }

//  /**
//   * Returns the next group in the table.  Returns 'null' if there are no
//   * more groups in the table.
//   */
//  public VirtualTable nextGroup() {
//    if (last_row_index != -1) {
//
//      IntegerVector new_list = new IntegerVector(8);
//
//      int row_index = last_row_index;
//      int top_row_index = row_index;
//
//      boolean equal = true;
//
//      do {
//        new_list.addInt(row_index);
//        if (!row_enum.hasMoreRows()) {
//          break;
//        }
//        row_index = row_enum.nextRowIndex();
//
//        equal = true;
//        for (int i = 0; i < columns.length && equal; ++i) {
//          TObject cell = table.getCellContents(columns[i], top_row_index);
//          equal = equal &&
//              table.compareCellTo(cell, columns[i], row_index) == Table.EQUAL;
//        }
//
//      } while (equal);
//
//      if (!equal) {
//        last_row_index = row_index;
//      }
//      else {
//        last_row_index = -1;
//      }
//
//
//      VirtualTable vtable = new VirtualTable(table);
//      vtable.set(table, new_list);
//      return vtable;
//
//    }
//    else {
//      return null;
//    }
//
//
//  }
//
//
//
//  public static final VirtualTable[] group(Table table, String[] groups) {
//
//
//    GroupHelper g_help = new GroupHelper(table, groups);
//    ArrayList list = new ArrayList();
//    VirtualTable tab = g_help.nextGroup();
//    while (tab != null) {
//      list.add(tab);
//      tab = g_help.nextGroup();
//    }
//
//    // Make into an array
//    VirtualTable[] table_array = (VirtualTable[])
//                                   list.toArray(new VirtualTable[list.size()]);
//    return table_array;
//
//  }


}
