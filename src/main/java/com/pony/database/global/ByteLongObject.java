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

import java.io.IOException;
import java.io.InputStream;

/**
 * A byte array that can be transferred between the client and server.  This
 * is used for transferring BLOB data to/from the database engine.
 *
 * @author Tobias Downer
 */

public class ByteLongObject implements java.io.Serializable, BlobAccessor {

    static final long serialVersionUID = -6843780673892019530L;

    /**
     * The binary data.
     */
    private final byte[] data;

    /**
     * Constructor.
     */
    public ByteLongObject(byte[] from, int offset, int length) {
        data = new byte[length];
        System.arraycopy(from, offset, data, 0, length);
    }

    public ByteLongObject(byte[] from) {
        this(from, 0, from.length);
    }

    public ByteLongObject(InputStream in, int length) throws IOException {
        data = new byte[length];
        int i = 0;
        while (i < length) {
            int read = in.read(data, i, length - i);
            if (read == -1) {
                throw new IOException("Premature end of stream.");
            }
            i += read;
        }
    }

    /**
     * Returns the size of the data in this object.
     */
    public int length() {
        return data.length;
    }

    /**
     * Returns the byte at offset 'n' into the binary object.
     */
    public byte getByte(int n) {
        return data[n];
    }

    /**
     * Returns the internal byte[] of this binary object.  Care needs to be
     * taken when handling this object because altering the contents will
     * change this object.
     */
    public byte[] getByteArray() {
        return data;
    }

    /**
     * Returns an InputStream that allows us to read the entire byte long object.
     */
    public InputStream getInputStream() {
        return new BLOBInputStream();
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        if (data == null) {
            buf.append("[ BLOB (NULL) ]");
        } else {
            buf.append("[ BLOB size=");
            buf.append(data.length);
            buf.append(" ]");
        }
        return new String(buf);
    }

    /**
     * Inner class that encapsulates the byte long object in an input stream.
     */
    private class BLOBInputStream extends InputStream {

        private int index;

        public BLOBInputStream() {
            index = 0;
        }

        public int read() throws IOException {
            if (index >= length()) {
                return -1;
            }
            int b = ((int) getByte(index)) & 0x0FF;
            ++index;
            return b;
        }

        public int read(byte[] buf, int off, int len) throws IOException {
            // As per the InputStream specification.
            if (len == 0) {
                return 0;
            }

            int size = length();
            int to_read = Math.min(len, size - index);

            if (to_read <= 0) {
                // Nothing can be read
                return -1;
            }

            System.arraycopy(data, index, buf, off, to_read);
            index += to_read;

            return to_read;
        }

    }

}
