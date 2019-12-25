/*
 * Pony SQL Database ( http://www.ponysql.ru/ )
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

import java.io.Reader;
import java.io.InputStream;
import java.io.IOException;

/**
 * A java.io.Reader implementation that wraps around an ascii input stream
 * (8-bit per char stream).
 *
 * @author Tobias Downer
 */

public final class AsciiReader extends Reader {

    /**
     * The 8-bit per character Ascii input straem.
     */
    private InputStream input;

    /**
     * Constructs the reader.
     */
    public AsciiReader(InputStream input) {
        this.input = input;
    }

    // ---------- Implemented from Reader ----------

    public int read() throws IOException {
        int v = input.read();
        if (v == -1) {
            return -1;
        } else {
            return (char) v;
        }
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
            int v = input.read();
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
        return input.skip(n);
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

