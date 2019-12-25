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

/**
 * A wrapper for an array of byte[].  This provides various functions for
 * altering the state of the buffer.
 *
 * @author Tobias Downer
 */

public final class ByteBuffer {

    /**
     * The byte[] array itself.
     */
    private final byte[] buf;

    /**
     * The current position in the array.
     */
    private int pos;

    /**
     * The length of the buf array.
     */
    private final int lim;

    /**
     * Constructs the buffer.
     */
    public ByteBuffer(byte[] buf, int offset, int length) {
        this.buf = buf;
        this.lim = length;
        this.pos = offset;
    }

    public ByteBuffer(byte[] buf) {
        this(buf, 0, buf.length);
    }

    /**
     * Sets the position in to the buffer.
     */
    public void position(int position) {
        this.pos = position;
    }

    /**
     * Returns the current position.
     */
    public int position() {
        return pos;
    }

    /**
     * Returns the limit of this buffer.
     */
    public int limit() {
        return lim;
    }

    /**
     * Puts a byte array into the buffer.
     */
    public ByteBuffer put(byte[] b, int offset, int length) {
        System.arraycopy(b, offset, buf, pos, length);
        position(pos + length);
        return this;
    }

    public ByteBuffer put(byte[] b) {
        return put(b, 0, b.length);
    }

    /**
     * Puts a ByteBuffer in to this buffer.
     */
    public ByteBuffer put(ByteBuffer buffer) {
        return put(buffer.buf, buffer.pos, buffer.lim);
    }

    /**
     * Gets a byte array from the buffer.
     */
    public ByteBuffer get(byte[] b, int offset, int length) {
        System.arraycopy(buf, pos, b, offset, length);
        position(pos + length);
        return this;
    }

    /**
     * Puts/Gets an integer into the buffer at the current position.
     */
    public ByteBuffer putInt(int v) {
        ByteArrayUtil.setInt(v, buf, pos);
        position(pos + 4);
        return this;
    }

    public int getInt() {
        int v = ByteArrayUtil.getInt(buf, pos);
        position(pos + 4);
        return v;
    }

    /**
     * Puts/Gets a byte into the buffer at the current position.
     */
    public ByteBuffer putByte(byte v) {
        buf[pos] = v;
        ++pos;
        return this;
    }

    public byte getByte() {
        byte b = buf[pos];
        ++pos;
        return b;
    }

    /**
     * Puts/Gets a short into the buffer at the current position.
     */
    public ByteBuffer putShort(short v) {
        ByteArrayUtil.setShort(v, buf, pos);
        position(pos + 2);
        return this;
    }

    public short getShort() {
        short v = ByteArrayUtil.getShort(buf, pos);
        position(pos + 2);
        return v;
    }


}
