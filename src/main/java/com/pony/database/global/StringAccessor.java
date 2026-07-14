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

package com.pony.database.global;

import java.io.Reader;

/**
 * An interface used by the engine to access and process strings.  This
 * interface allows us to access the contents of a string that may be
 * implemented in several different ways.  For example, a string may be
 * represented as a java.lang.String object in memeory, or it may be
 * represented as an ASCII sequence in a store.
 *
 * @author Tobias Downer
 */

public interface StringAccessor {

    /**
     * Returns the number of characters in the string.
     */
    int length();

    /**
     * Returns a Reader that allows the string to be read sequentually from
     * start to finish.
     */
    Reader getReader();

    /**
     * Returns this string as a java.lang.String object.  Some care may be
     * necessary with this call because a very large string will require a lot
     * space on the heap.
     */
    String toString();

}

