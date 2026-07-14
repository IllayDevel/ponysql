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

import java.io.*;

/**
 * This is a ByteArrayOutputStream that allows access to the underlying byte
 * array.  It can be instantiated, and then used over and over as a temporary
 * buffer between the writeTo methods and the underlying random access file
 * stream.
 * <p>
 * @author Tobias Downer
 */

public final class CellBufferOutputStream extends ByteArrayOutputStream {

    /**
     * The Constructor.
     */
    public CellBufferOutputStream(int length) {
        super(length);
    }

    /**
     * Returns the underlying stream you should not use the stream while you have
     * a handle on this reference.
     */
    public byte[] getByteArray() {
        return buf;
    }

    /**
     * Sets the pointer to specified point in the array.
     */
    public void seek(int pointer) {
        count = pointer;
    }

}
