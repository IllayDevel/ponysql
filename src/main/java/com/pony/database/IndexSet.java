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

import com.pony.util.IntegerListInterface;

/**
 * A set of list of indexes.  This will often expose an isolated snapshot of a
 * set of indices for a table.
 *
 * @author Tobias Downer
 */

public interface IndexSet {

    /**
     * Returns a mutable object that implements IntegerListInterface for the
     * given index number in this set of indices.
     */
    IntegerListInterface getIndex(int n);

    /**
     * Cleans up and disposes the resources associated with this set of index.
     */
    void dispose();

}
