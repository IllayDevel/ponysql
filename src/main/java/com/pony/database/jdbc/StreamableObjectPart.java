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

package com.pony.database.jdbc;

/**
 * Represents a response from the server for a section of a streamable object.
 * A streamable object can always be represented as a byte[] array and is
 * limited to String (as 2-byte unicode) and binary data types.
 *
 * @author Tobias Downer
 */

public class StreamableObjectPart {

    /**
     * The byte[] array that is the contents of the cell from the server.
     */
    private final byte[] part_contents;

    /**
     * Constructs the ResultCellPart.  Note that the 'contents' byte array must
     * be immutable.
     */
    public StreamableObjectPart(byte[] contents) {
        this.part_contents = contents;
    }

    /**
     * Returns the contents of this ResultCellPart.
     */
    public byte[] getContents() {
        return part_contents;
    }

}

