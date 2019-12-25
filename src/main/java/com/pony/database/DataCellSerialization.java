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

package com.pony.database;

import com.pony.database.global.*;
import com.pony.util.BigNumber;

import java.util.zip.*;
import java.util.Date;
import java.io.*;

/**
 * An object that manages the serialization and deserialization of objects
 * to the database file system.  This object maintains a buffer that stores
 * intermediate serialization information as objects are written.
 *
 * @author Tobias Downer
 */

final class DataCellSerialization extends ByteArrayOutputStream
        implements CellInput {

    /**
     * A Deflater and Inflater used to compress and uncompress the size of data
     * fields put into the store.
     */
    private Deflater deflater;
    private Inflater inflater;
    private byte[] compress_buf;
    private int compress_length;

    /**
     * If true, when writing out use the compressed form.
     */
    private boolean use_compressed;

    /**
     * The type of object.
     */
    private short type;

    /**
     * Set to true if null.
     */
    private boolean is_null;

    /**
     * Constructor.
     */
    DataCellSerialization() {
        super(1024);
    }


    /**
     * Returns the number of bytes to skip on the stream to go past the
     * next serialization.
     */
    int skipSerialization(CellInput din) throws IOException {
        int len = din.readInt();
        return len - 4;
    }

    /**
     * Reads input from the given CellInput object.
     */
    Object readSerialization(CellInput din) throws IOException {

        count = 0;

        // Read the length first,
        int len = din.readInt();
        short s = din.readShort();
        type = (short) (s & 0x0FFF);
        is_null = (s & 0x02000) != 0;
        use_compressed = (s & 0x04000) != 0;

        // If we are compressed...
        if (use_compressed) {
            // Uncompress it,
            int uncompressed_len = din.readInt();
            if (buf.length < uncompressed_len) {
                buf = new byte[uncompressed_len];
            }

            // Write data to the compressed buffer
            compress_length = len - 4 - 2 - 4;
            if (compress_buf == null || compress_buf.length < compress_length) {
                compress_buf = new byte[compress_length];
            }
            din.readFully(compress_buf, 0, compress_length);

            if (inflater == null) {
                inflater = new Inflater();
            }
            inflater.reset();
            inflater.setInput(compress_buf, 0, compress_length);
            int inflate_count;
            try {
                inflate_count = inflater.inflate(buf, 0, uncompressed_len);
            } catch (DataFormatException e) {
                throw new RuntimeException(e.getMessage());
            }

            din = this;

        }

        return readFromCellInput(din);
    }

    /**
     * Creates a BigNumber object used to store a numeric value in the database.
     */
    private BigNumber createBigNumber(byte[] buf, int scale, byte state) {
        // Otherwise generate the number from the data given.
        return BigNumber.fromData(buf, scale, state);
    }

    /**
     * Reads an object from the given CellInput.  No type information is included
     * with the returned object so it must be wrapped in a TObject.  Returns
     * null if the object stored was null.
     */
    private Object readFromCellInput(CellInput din) throws IOException {

        // If null byte is 1 then return null data cell.
        if (is_null) {
            return null;
        } else {
            // This type isn't actually serialized anymore, but we must understand
            // how to deserialize it because of older database formats.
            if (type == Types.DB_NUMERIC) {
                int scale = din.readShort();
                int num_len = din.readInt();
                byte[] buf = new byte[num_len];
                din.readFully(buf, 0, num_len);

                return createBigNumber(buf, scale, (byte) 0);
            } else if (type == Types.DB_NUMERIC_EXTENDED) {
                byte state = din.readByte();
                int scale = din.readShort();
                int num_len = din.readInt();
                byte[] buf = new byte[num_len];
                din.readFully(buf, 0, num_len);

                return createBigNumber(buf, scale, state);
            } else if (type == Types.DB_STRING) {
                int str_length = din.readInt();
                // No length string is a static to save memory.
                if (str_length == 0) {
                    return "";
                }

                String dastr = din.readChars(str_length);
                // NOTE: We intern the string to save memory.
                return dastr.intern();
            } else if (type == Types.DB_BOOLEAN) {
                if (din.readByte() == 0) {
                    return Boolean.FALSE;
                } else {
                    return Boolean.TRUE;
                }
            } else if (type == Types.DB_TIME) {
                return new java.util.Date(din.readLong());
            } else if (type == Types.DB_BLOB) {
                int blob_length = din.readInt();
                // Intern to save memory
                if (blob_length == 0) {
                    return EMPTY_BYTE_LONG_OBJECT;
                }

                byte[] buf = new byte[blob_length];
                din.readFully(buf, 0, blob_length);

                return new ByteLongObject(buf);
            } else if (type == Types.DB_OBJECT) {
                int blob_length = din.readInt();

                byte[] buf = new byte[blob_length];
                din.readFully(buf, 0, blob_length);

                return new ByteLongObject(buf);
            } else {
                throw new Error("Don't understand type: " + type);
            }

        }

    }

    /**
     * Writes the current serialized data buffer to the output stream.
     */
    void writeSerialization(DataOutputStream out) throws IOException {
        int len = use_compressed ? (compress_length + 4) : count;
        // size + (type | null | compressed)
        len += 4 + 2;
        out.writeInt(len);
        short s = type;
        if (is_null) {
            s |= 0x02000;
        }
        if (use_compressed) {
            s |= 0x04000;
        }
        out.writeShort(s);

        // Write out the data.
        if (use_compressed) {
            // If compressed, must write out uncompressed size first.
            out.writeInt(count);
            out.write(compress_buf, 0, compress_length);
        } else {
            out.write(buf, 0, count);
        }

        // And that's it!
    }

    /**
     * Sets this up with a TObject to serialize.
     */
    void setToSerialize(TObject cell) throws IOException {

        is_null = false;
        count = 0;
        use_compressed = false;

        TType ttype = cell.getTType();
        if (ttype instanceof TStringType) {
            type = Types.DB_STRING;
        } else if (ttype instanceof TNumericType) {
            // NOTE: We set type to DB_NUMERIC_EXTENDED which includes support for
            //   NaN, negative infinity and positive infinity.
            type = Types.DB_NUMERIC_EXTENDED;
        } else if (ttype instanceof TBooleanType) {
            type = Types.DB_BOOLEAN;
        } else if (ttype instanceof TDateType) {
            type = Types.DB_TIME;
        } else if (ttype instanceof TBinaryType) {
            type = Types.DB_BLOB;
        } else if (ttype instanceof TJavaObjectType) {
            type = Types.DB_OBJECT;
        } else {
            throw new Error("Couldn't handle type: " + ttype.getClass());
        }

        if (cell.isNull()) {
            is_null = true;
            return;
        }

        Object ob = cell.getObject();

        // Write the serialized form to the buffer,
        writeToBuffer(cell);

        // Should we compress?

        // If it's a string, blob or serialized object, we may want to compress it,
        TType type = cell.getTType();
        if (type instanceof TStringType ||
                type instanceof TBinaryType ||
                type instanceof TJavaObjectType) {
            int length = count;
            // Any strings > 150 are compressed
            if (length > 150) {

                if (deflater == null) {
                    deflater = new Deflater();
                }

                deflater.setInput(buf, 0, length);
                deflater.finish();

                if (compress_buf == null || compress_buf.length < length) {
                    compress_buf = new byte[length];
                }
                compress_length = deflater.deflate(compress_buf);
                deflater.reset();

                if (compress_length < length) {
                    use_compressed = true;
                }
            }
        }

    }

    /**
     * Writes the TObject to the data buffer in this object.
     */
    private void writeToBuffer(TObject cell) throws IOException {

        Object ob = cell.getObject();

        if (ob instanceof BigNumber) {
            BigNumber ddc = (BigNumber) ob;
            byte[] buf = ddc.toByteArray();
            writeByte(ddc.getState());
            writeShort((short) ddc.getScale());
            writeInt(buf.length);
            write(buf);
        } else if (ob instanceof String) {
            String str = (String) ob;
            writeInt(str.length());
            writeChars(str);
        } else if (ob instanceof Boolean) {
            Boolean bool = (Boolean) ob;
            writeByte((byte) (bool ? 1 : 0));
        } else if (ob instanceof java.util.Date) {
            Date date = (Date) ob;
            writeLong(date.getTime());
        } else if (ob instanceof ByteLongObject) {
            ByteLongObject blob = (ByteLongObject) ob;
            writeInt(blob.length());
            write(blob.getByteArray());
        } else {
            throw new Error("Don't know how to serialize class " + ob.getClass());
        }

    }

    public final void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    public final void writeByte(int v) throws IOException {
        write(v);
    }

    public final void writeShort(int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public final void writeChar(int v) throws IOException {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public final void writeInt(int v) throws IOException {
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public final void writeLong(long v) throws IOException {
        write((int) (v >>> 56) & 0xFF);
        write((int) (v >>> 48) & 0xFF);
        write((int) (v >>> 40) & 0xFF);
        write((int) (v >>> 32) & 0xFF);
        write((int) (v >>> 24) & 0xFF);
        write((int) (v >>> 16) & 0xFF);
        write((int) (v >>> 8) & 0xFF);
        write((int) (v >>> 0) & 0xFF);
    }

    public final void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; ++i) {
            int v = s.charAt(i);
            write((v >>> 8) & 0xFF);
            write((v >>> 0) & 0xFF);
        }
    }


    // ---------- Implemented from CellInput ----------

    public int read() throws IOException {
        return buf[count++] & 0x0FF;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (len <= 0) {
            return 0;
        }
        System.arraycopy(buf, count, b, off, len);
        count += len;
        return len;
    }

    public long skip(long n) throws IOException {
        if (n < 0) {
            return 0;
        }
        count += n;
        return n;
    }

    public int available() throws IOException {
        throw new Error("Not supported");
    }

    public void mark(int readAheadLimit) throws IOException {
        throw new Error("Not supported");
    }

// [ Function clash here but it should be okay ]
//  public void reset() throws IOException {
//    throw new Error("Not supported");
//  }

    public void close() throws IOException {
        throw new Error("Not supported");
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

    private char[] char_buffer;

    public String readChars(int length) throws IOException {
        if (length <= 8192) {
            if (char_buffer == null) {
                char_buffer = new char[8192];
            }
            for (int i = 0; i < length; ++i) {
                char_buffer[i] = readChar();
            }
            return new String(char_buffer, 0, length);
        } else {
            StringBuffer chrs = new StringBuffer(length);
            for (int i = length; i > 0; --i) {
                chrs.append(readChar());
            }
            return new String(chrs);
        }
    }

    public int readInt() throws IOException {
        int ch1 = read();
        int ch2 = read();
        int ch3 = read();
        int ch4 = read();
        return (int) ((ch1 << 24) + (ch2 << 16) +
                (ch3 << 8) + (ch4 << 0));
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

    // ---------- Some statics -----------

    /**
     * A 0 size ByteLongObject object.
     */
    private static final ByteLongObject EMPTY_BYTE_LONG_OBJECT =
            new ByteLongObject(new byte[0]);

}
