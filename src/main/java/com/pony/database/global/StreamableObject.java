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

/**
 * An object that is streamable (such as a long binary object, or
 * a long string object).  This is passed between client and server and
 * contains basic primitive information about the object it represents.  The
 * actual contents of the object itself must be obtained through other
 * means (see com.pony.database.jdbc.DatabaseInterface).
 *
 * @author Tobias Downer
 */

public final class StreamableObject {

    /**
     * The type of the object.
     */
    private final byte type;

    /**
     * The size of the object in bytes.
     */
    private final long size;

    /**
     * The identifier that identifies this object.
     */
    private final long id;

    /**
     * Constructs the StreamableObject.
     */
    public StreamableObject(byte type, long size, long id) {
        this.type = type;
        this.size = size;
        this.id = id;
    }

    /**
     * Returns the type of object this stub represents.  Returns 1 if it
     * represents 2-byte unicde character object, 2 if it represents binary data.
     */
    public byte getType() {
        return type;
    }

    /**
     * Returns the size of the object stream, or -1 if the size is unknown.  If
     * this represents a unicode character string, you would calculate the total
     * characters as size / 2.
     */
    public long getSize() {
        return size;
    }

    /**
     * Returns an identifier that can identify this object within some context.
     * For example, if this is a streamable object on the client side, then the
     * identifier might be the value that is able to retreive a section of the
     * streamable object from the DatabaseInterface.
     */
    public long getIdentifier() {
        return id;
    }

}

