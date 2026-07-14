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

/**
 * A RowEnumeration implementation that represents a sequence of rows that
 * can be referenced in incremental order between 0 and row_count (exclusive).
 * A Table that returns a SimpleRowEnumeration is guarenteed to provide valid
 * TObject values via the 'getCellContents' method between rows 0 and
 * getRowCount().
 *
 * @author Tobias Downer
 */

public final class SimpleRowEnumeration implements RowEnumeration {

    /**
     * The current index.
     */
    private int index = 0;

    /**
     * The number of rows in the enumeration.
     */
    final int row_count_store;

    /**
     * Constructs the RowEnumeration.
     */
    public SimpleRowEnumeration(int row_count) {
        row_count_store = row_count;
    }

    public final boolean hasMoreRows() {
        return (index < row_count_store);
    }

    public final int nextRowIndex() {
        ++index;
        return index - 1;
    }

}

