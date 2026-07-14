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

package com.pony.store;

import java.io.IOException;

/**
 * An interface for access the contents of an area of a store.  The area object
 * maintains a pointer that can be manipulated and read from.
 *
 * @author Tobias Downer
 */

public interface Area {

    /**
     * Returns the unique identifier that represents this area.
     */
    long getID();

    /**
     * Returns the current position of the pointer within the area.  The position
     * starts at beginning of the area.
     */
    int position();

    /**
     * Returns the capacity of the area.
     */
    int capacity();

    /**
     * Sets the position within the area.
     */
    void position(int position) throws IOException;

    /**
     * Copies 'size' bytes from the current position of this Area to the
     * destination AreaWriter.
     */
    void copyTo(AreaWriter destination_writer, int size) throws IOException;

    // ---------- The get methods ----------
    // Note that these methods will all increment the position by the size of the
    // element read.  For example, 'getInt' will increment the position by 4.

    byte get() throws IOException;

    void get(byte[] buf, int off, int len) throws IOException;

    short getShort() throws IOException;

    int getInt() throws IOException;

    long getLong() throws IOException;

    char getChar() throws IOException;

}

