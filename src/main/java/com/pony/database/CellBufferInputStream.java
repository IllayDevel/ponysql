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

package com.pony.database;

//import java.io.ByteArrayInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * This is a reusable cell stream object that is extended from the
 * ByteArrayInputStream class, which provides methods for reusing the object
 * on a different byte[] arrays.  It is used as an efficient way of reading
 * cell information from a binary fixed length cell type file.
 * <p>
 * It would usually be wrapped in a DataInputStream object.
 * <p>
 * @author Tobias Downer
 */

final class CellBufferInputStream extends InputStream implements CellInput {

//  /**
//   * A wrapped DataInputStream over this stream.
//   */
//  private DataInputStream wrapped_data;

    private byte[] buf;
    private int pos;
    private int mark = 0;
    private int count;

    /**
     * The Constructor.
     */
    CellBufferInputStream() {
        this.buf = null;
        this.pos = 0;
        this.count = 0;
    }

    /**
     * Sets up the stream to the start of the underlying array.
     */
    void setArray(byte[] new_buffer) {
        buf = new_buffer;
        pos = 0;
        mark = 0;
        count = new_buffer.length;
    }

    /**
     * Sets up the stream to the underlying array with the given variables.
     */
    void setArray(byte[] new_buffer, int offset, int length) {
        buf = new_buffer;
        pos = offset;
        mark = 0;
        count = Math.min(new_buffer.length, length + offset);
    }

    /**
     * Sped up methods.
     */
    public int read() {
        return buf[pos++] & 0x0FF;
//    return (pos < count) ? (buf[pos++] & 0xff) : -1;
    }

    public int read(byte[] b, int off, int len) {
//    if (b == null) {
//      throw new NullPointerException();
//    } else if ((off < 0) || (off > b.length) || (len < 0) ||
//              ((off + len) > b.length) || ((off + len) < 0)) {
//      throw new IndexOutOfBoundsException();
//    }
        if (pos >= count) {
            return -1;
        }
        if (pos + len > count) {
            len = count - pos;
        }
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
        return len;
    }

    public long skip(long n) {
        if (pos + n > count) {
            n = count - pos;
        }
        if (n < 0) {
            return 0;
        }
        pos += n;
        return n;
    }

    public int available() {
        return count - pos;
    }

    public void mark(int readAheadLimit) {
        mark = pos;
    }

    public void reset() {
        pos = mark;
    }

    public void close() throws IOException {
    }


    // ---------- Implemented from DataInput ----------

    public void readFully(byte[] b) throws IOException {
        read(b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        read(b, off, len);
    }

    public int skipBytes(int n) throws IOException {
        return (int) skip(n);
    }

    public boolean readBoolean() throws IOException {
        return (read() != 0);
    }

    public byte readByte() throws IOException {
        return (byte) read();
    }

    public int readUnsignedByte() throws IOException {
        return read();
    }

    public short readShort() throws IOException {
        int ch1 = read();
        int ch2 = read();
        return (short) ((ch1 << 8) + (ch2 << 0));
    }

    public int readUnsignedShort() throws IOException {
        int ch1 = read();
        int ch2 = read();
        return (ch1 << 8) + (ch2 << 0);
    }

    public char readChar() throws IOException {
        int ch1 = read();
        int ch2 = read();
        return (char) ((ch1 << 8) + (ch2 << 0));
    }

    private final char[] char_buffer = new char[8192];

    public String readChars(int length) throws IOException {
        if (length <= char_buffer.length) {
            for (int i = 0; i < length; ++i) {
                char_buffer[i] = (char) (((buf[pos++] & 0x0FF) << 8) +
                        ((buf[pos++] & 0x0FF) << 0));
            }
            return new String(char_buffer, 0, length);
        } else {
            StringBuffer chrs = new StringBuffer(length);
            for (int i = length; i > 0; --i) {
                chrs.append((char) (((buf[pos++] & 0x0FF) << 8) +
                        ((buf[pos++] & 0x0FF) << 0)));
            }
            return new String(chrs);
        }
    }

    public int readInt() throws IOException {
        return ((buf[pos++] & 0x0FF) << 24) +
                ((buf[pos++] & 0x0FF) << 16) +
                ((buf[pos++] & 0x0FF) << 8) +
                ((buf[pos++] & 0x0FF) << 0);
    }

    public long readLong() throws IOException {
        return ((long) (readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public String readLine() throws IOException {
        throw new Error("Not implemented.");
    }

    public String readUTF() throws IOException {
        throw new Error("Not implemented.");
    }


//  /**
//   * Returns a wrapped DataInputStream for this stream.  This is a
//   * convenience, but will improve on efficiency of a
//   * 'new DataInputStream(...)' type allocation.
//   */
//  DataInputStream getDataInputStream() {
//    if (wrapped_data != null) {
//      return wrapped_data;
//    }
//    return wrapped_data = new DataInputStream(this);
//  }

}
