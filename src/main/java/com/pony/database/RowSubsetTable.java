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
 * A virtual table that exposes a contiguous row window of another table.
 */
public final class RowSubsetTable extends VirtualTable {

    public RowSubsetTable(Table table, int offset, int limit) {
        super(table);
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be greater than or equal to 0");
        }
        if (limit < -1) {
            throw new IllegalArgumentException("limit must be greater than or equal to -1");
        }
        set(table, selectRows(table, offset, limit));
    }

    private static IntegerVector selectRows(Table table, int offset, int limit) {
        IntegerVector rows = new IntegerVector();
        RowEnumeration row_enum = table.rowEnumeration();

        int skipped = 0;
        while (skipped < offset && row_enum.hasMoreRows()) {
            row_enum.nextRowIndex();
            ++skipped;
        }

        int selected = 0;
        while ((limit < 0 || selected < limit) && row_enum.hasMoreRows()) {
            rows.addInt(row_enum.nextRowIndex());
            ++selected;
        }

        return rows;
    }
}
