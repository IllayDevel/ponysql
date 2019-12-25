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

