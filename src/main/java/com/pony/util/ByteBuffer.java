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
