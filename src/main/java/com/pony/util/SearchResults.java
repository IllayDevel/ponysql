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

/**
 * This object stores the result of a given search.  It provides information
 * object where in the set the found elements are, and the number of elements
 * in the set that match the search criteria.
 * <p>
 * @author Tobias Downer
 */

public final class SearchResults {

    /**
     * The index in the array of the found elements.
     */
    int found_index;

    /**
     * The number of elements in the array that match the given search criteria.
     */
    int found_count;

    /**
     * The Constructor.
     */
    public SearchResults() {
    }

    /**
     * Functions for querying information in the results.
     */
    public int getCount() {
        return found_count;
    }

    public int getIndex() {
        return found_index;
    }

}
