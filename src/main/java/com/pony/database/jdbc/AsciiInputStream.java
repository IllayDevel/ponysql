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

import java.io.*;

/**
 * An InputStream that converts a Reader to a plain ascii stream.  This
 * cuts out the top 8 bits of the unicode char.
 *
 * @author Tobias Downer
 */

class AsciiInputStream extends InputStream { // extends InputStreamFilter {

    private final Reader reader;

    public AsciiInputStream(Reader reader) {
        this.reader = reader;
    }

    public AsciiInputStream(String s) {
        this(new StringReader(s));
    }

    public int read() throws IOException {
        int i = reader.read();
        if (i == -1) return i;
        else return (i & 0x0FF);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        int end = off + len;
        int read_count = 0;
        for (int i = off; i < end; ++i) {
            int val = read();
            if (val == -1) {
                if (read_count == 0) {
                    return -1;
                } else {
                    return read_count;
                }
            }
            b[i] = (byte) val;
            ++read_count;
        }
        return read_count;
    }

    public long skip(long n) throws IOException {
        return reader.skip(n);
    }

    public int available() throws IOException {
        // NOTE: This is valid according to JDBC spec.
        return 0;
    }

    public void reset() throws IOException {
        reader.reset();
    }

}
