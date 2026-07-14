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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * A Reader implementation that wraps around a unicode encoded input stream
 * that encodes each unicode character as 2 bytes.  See
 * UnicodeToBinaryStream for the InputStream version of this class.
 *
 * @author Tobias Downer
 */

public final class BinaryToUnicodeReader extends Reader {

    /**
     * The wrapped InputStream.
     */
    private final InputStream input;

    /**
     * Constructor.  Note that we would typically assume that the given
     * InputStream employs some type of buffering and that calls to 'read' are
     * buffered and therefore work quickly.
     */
    public BinaryToUnicodeReader(InputStream input) {
        this.input = input;
    }

    // ---------- Implemented from Reader ----------

    public int read() throws IOException {
        int v1 = input.read();
        if (v1 == -1) {
            return -1;
        }
        int v2 = input.read();
        if (v2 == -1) {
            return -1;
        }

        return (v1 << 8) + v2;
    }

    public int read(char[] buf, int off, int len) throws IOException {
        if (len < 0) {
            throw new IOException("len < 0");
        }
        if (off < 0 || off + len > buf.length) {
            throw new IOException("Out of bounds.");
        }
        if (len == 0) {
            return 0;
        }
        int read = 0;
        while (len > 0) {
            int v = read();
            if (v == -1) {
                if (read == 0) {
                    return -1;
                } else {
                    return read;
                }
            }
            buf[off] = (char) v;
            ++off;
            ++read;
            --len;
        }
        return read;
    }

    public long skip(long n) throws IOException {
        return input.skip(n * 2) / 2;
    }

    public boolean ready() throws IOException {
        return false;
    }

    public void mark(int readAheadLimit) throws IOException {
        input.mark(readAheadLimit);
    }

    public void reset() throws IOException {
        input.reset();
    }

    public void close() throws IOException {
        input.close();
    }

}

