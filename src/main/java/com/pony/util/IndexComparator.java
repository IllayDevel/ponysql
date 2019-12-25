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

package com.pony.util;

/**
 * A comparator that is used within BlockIntegerList that compares two int
 * values which are indices to data that is being compared.  For example, we
 * may have an BlockIntegerList that contains indices to cells in the column
 * of a table.  To make a sorted list, we use this comparator to lookup the
 * index values in the list for sorting and searching.
 *
 * @author Tobias Downer
 */

public interface IndexComparator {

    /**
     * Returns > 0 if the value pointed to by index1 is greater than 'val',
     * or &lt; 0 if the value pointed to by index 1 is less than 'val'.  If the
     * indexed value is equal to 'val', it returns 0.
     */
    int compare(int index1, Object val);

    /**
     * Returns >0 if the value pointed to by index1 is greater than the value
     * pointed to by index2, or &tl; 0 if the value pointed to by index 1 is less
     * than the value pointed to by index 2.  If the indexed value's are equal,
     * it returns 0.
     */
    int compare(int index1, int index2);

}
