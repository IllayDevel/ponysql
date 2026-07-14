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

import com.pony.util.IntegerVector;

/**
 * A table that is the cartesian product of two tables.  This provides
 * better memory-use and efficiency than a materialized table backed by a
 * VirtualTable.
 *
 * @author Tobias Downer
 */

public final class NaturallyJoinedTable extends JoinedTable {

    /**
     * The row counts of the left and right tables.
     */
    private final int left_row_count, right_row_count;

    /**
     * The lookup row set for the left and right tables.  Basically, these point
     * to each row in either the left or right tables.
     */
    private final IntegerVector left_set, right_set;
    private final boolean left_is_simple_enum, right_is_simple_enum;

    /**
     * Constructs the table.
     */
    public NaturallyJoinedTable(Table left, Table right) {
        super.init(new Table[]{left, right});

        left_row_count = left.getRowCount();
        right_row_count = right.getRowCount();

        // Build lookup tables for the rows in the parent tables if necessary
        // (usually it's not necessary).

        // If the left or right tables are simple enumerations, we can optimize
        // our access procedure,
        left_is_simple_enum =
                (left.rowEnumeration() instanceof SimpleRowEnumeration);
        right_is_simple_enum =
                (right.rowEnumeration() instanceof SimpleRowEnumeration);
        if (!left_is_simple_enum) {
            left_set = createLookupRowList(left);
        } else {
            left_set = null;
        }
        if (!right_is_simple_enum) {
            right_set = createLookupRowList(right);
        } else {
            right_set = null;
        }

    }

    /**
     * Creates a lookup list for rows in the given table.
     */
    private static IntegerVector createLookupRowList(Table t) {
        IntegerVector ivec = new IntegerVector();
        RowEnumeration en = t.rowEnumeration();
        while (en.hasMoreRows()) {
            int row_index = en.nextRowIndex();
            ivec.addInt(row_index);
        }
        return ivec;
    }

    /**
     * Given a row index between 0 and left table row count, this will return a
     * row index into the left table's row domain.
     */
    private int getLeftRowIndex(int row_index) {
        if (left_is_simple_enum) {
            return row_index;
        }
        return left_set.intAt(row_index);
    }

    /**
     * Given a row index between 0 and right table row count, this will return a
     * row index into the right table's row domain.
     */
    private int getRightRowIndex(int row_index) {
        if (right_is_simple_enum) {
            return row_index;
        }
        return right_set.intAt(row_index);
    }


    // ---------- Implemented from JoinedTable ----------

    public int getRowCount() {
        // Natural join row count is (left table row count * right table row count)
        return left_row_count * right_row_count;
    }

    protected int resolveRowForTableAt(int row_number, int table_num) {
        if (table_num == 0) {
            return getLeftRowIndex(row_number / right_row_count);
        } else {
            return getRightRowIndex(row_number % right_row_count);
        }
    }

    protected void resolveAllRowsForTableAt(
            IntegerVector row_set, int table_num) {
        boolean pick_right_table = (table_num == 1);
        for (int n = row_set.size() - 1; n >= 0; --n) {
            int aa = row_set.intAt(n);
            // Reverse map row index to parent domain
            int parent_row;
            if (pick_right_table) {
                parent_row = getRightRowIndex(aa % right_row_count);
            } else {
                parent_row = getLeftRowIndex(aa / right_row_count);
            }
            row_set.setIntAt(parent_row, n);
        }
    }

}

