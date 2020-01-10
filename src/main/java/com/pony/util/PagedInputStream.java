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

package com.pony.util;

import java.io.InputStream;
import java.io.IOException;

/**
 * An implementation of InputStream that reads data from an underlying
 * representation in fixed sized pages.  This object maintains a single buffer
 * that is the size of a page.  This implementation supports 'skip' and
 * buffered access to the data.
 * <p>
 * The only method that needs to be implemented is the 'readPageContent'
 * method.
 *
 * @author Tobias Downer
 */

public abstract class PagedInputStream extends InputStream {

    /**
     * The size of the buffer page.
     */
    private final int BUFFER_SIZE;

    /**
     * The current position in the stream.
     */
    private long position;

    /**
     * The total size of the underlying dataset.
     */
    private final long size;

    /**
     * The start buffer position.
     */
    private long buffer_pos;

    /**
     * The buffer.
     */
    private final byte[] buf;

    /**
     * Last marked position.
     */
    private long mark_position;

    /**
     * Constructs the input stream.
     *
     * @param page_size the size of the pages when accessing the underlying
     *   stream.
     * @param total_size the total size of the underlying data set.
     */
    public PagedInputStream(int page_size, long total_size) {
        this.BUFFER_SIZE = page_size;
        this.position = 0;
        this.size = total_size;
        this.mark_position = 0;
        this.buf = new byte[BUFFER_SIZE];
        buffer_pos = -1;
    }

    /**
     * Reads the page at the given offset in the underlying data into the given
     * byte[] array.  The 'pos' variable given is guarenteed to be a multiple of
     * buffer_size.  For example, the first access will be to pos = 0, the
     * second access to pos = BUFFER_SIZE, the third access to pos =
     * BUFFER_SIZE * 2, etc.  'length' will always be either BUFFER_SIZE or a
     * value smaller than BUFFER_SIZE if the page containing the end of the
     * stream is read.
     */
    protected abstract void readPageContent(byte[] buf, long pos, int length)
            throws IOException;

    /**
     * Fills the buffer with data from the blob at the given position.  A buffer
     * may be partially filled if the end is reached.
     */
    private void fillBuffer(long pos) throws IOException {
        final long read_pos = (pos / BUFFER_SIZE) * BUFFER_SIZE;
        int to_read = (int) Math.min(BUFFER_SIZE, (size - read_pos));
        if (to_read > 0) {
            readPageContent(buf, read_pos, to_read);
            buffer_pos = read_pos;
        }
    }

    // ---------- Implemented from InputStream ----------

    public int read() throws IOException {
        if (position >= size) {
            return -1;
        }

        if (buffer_pos == -1) {
            fillBuffer(position);
        }

        int p = (int) (position - buffer_pos);
        int v = ((int) buf[p]) & 0x0FF;

        ++position;
        // Fill the next part of the buffer?
        if (p + 1 >= BUFFER_SIZE) {
            fillBuffer(buffer_pos + BUFFER_SIZE);
        }

        return v;
    }

    public int read(byte[] read_buf, int off, int len) throws IOException {
        if (len <= 0) {
            return 0;
        }

        if (buffer_pos == -1) {
            fillBuffer(position);
        }

        int p = (int) (position - buffer_pos);
        long buffer_end = Math.min(buffer_pos + BUFFER_SIZE, size);
        int to_read = (int) Math.min(len, buffer_end - position);
        if (to_read <= 0) {
            return -1;
        }
        int has_read = 0;
        while (to_read > 0) {
            System.arraycopy(buf, p, read_buf, off, to_read);
            has_read += to_read;
            p += to_read;
            off += to_read;
            len -= to_read;
            position += to_read;
            if (p >= BUFFER_SIZE) {
                fillBuffer(buffer_pos + BUFFER_SIZE);
                p -= BUFFER_SIZE;
            }
            buffer_end = Math.min(buffer_pos + BUFFER_SIZE, size);
            to_read = (int) Math.min(len, buffer_end - position);
        }
        return has_read;
    }

    public long skip(long n) throws IOException {
        long act_skip = Math.min(n, size - position);

        if (n < 0) {
            throw new IOException("Negative skip");
        }
        position += act_skip;
        if (buffer_pos == -1 || (position - buffer_pos) > BUFFER_SIZE) {
            fillBuffer((position / BUFFER_SIZE) * BUFFER_SIZE);
        }

        return act_skip;
    }

    public int available() throws IOException {
        return (int) Math.min(Integer.MAX_VALUE, (size - position));
    }

    public void close() throws IOException {
    }

    public void mark(int limit) {
        mark_position = position;
    }

    public void reset() {
        position = mark_position;
        long fill_pos = (position / BUFFER_SIZE) * BUFFER_SIZE;
        if (fill_pos != buffer_pos) {
            try {
                fillBuffer(fill_pos);
            } catch (IOException e) {
                throw new Error(e.getMessage());
            }
        }
    }

    public boolean markSupported() {
        return true;
    }

}

