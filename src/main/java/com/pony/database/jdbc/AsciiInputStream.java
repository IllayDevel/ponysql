/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
