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

package com.pony.util;

/**
 * Static utilities for byte arrays.
 *
 * @author Tobias Downer
 */

public class ByteArrayUtil {

    /**
     * Returns the chart at the given offset of the byte array.
     */
    public static char getChar(byte[] arr, int offset) {
        int c1 = (((int) arr[offset + 0]) & 0x0FF);
        int c2 = (((int) arr[offset + 1]) & 0x0FF);
        return (char) ((c1 << 8) + (c2));
    }

    /**
     * Sets the short at the given offset of the byte array.
     */
    public static void setChar(char value, byte[] arr, int offset) {
        arr[offset + 0] = (byte) ((value >>> 8) & 0x0FF);
        arr[offset + 1] = (byte) ((value >>> 0) & 0x0FF);
    }

    /**
     * Returns the short at the given offset of the byte array.
     */
    public static short getShort(byte[] arr, int offset) {
        int c1 = (((int) arr[offset + 0]) & 0x0FF);
        int c2 = (((int) arr[offset + 1]) & 0x0FF);
        return (short) ((c1 << 8) + (c2));
    }

    /**
     * Sets the short at the given offset of the byte array.
     */
    public static void setShort(short value, byte[] arr, int offset) {
        arr[offset + 0] = (byte) ((value >>> 8) & 0x0FF);
        arr[offset + 1] = (byte) ((value >>> 0) & 0x0FF);
    }

    /**
     * Returns the int at the given offset of the byte array.
     */
    public static int getInt(byte[] arr, int offset) {
        int c1 = (((int) arr[offset + 0]) & 0x0FF);
        int c2 = (((int) arr[offset + 1]) & 0x0FF);
        int c3 = (((int) arr[offset + 2]) & 0x0FF);
        int c4 = (((int) arr[offset + 3]) & 0x0FF);
        return (c1 << 24) + (c2 << 16) + (c3 << 8) + (c4);
    }

    /**
     * Sets the int at the given offset of the byte array.
     */
    public static void setInt(int value, byte[] arr, int offset) {
        arr[offset + 0] = (byte) ((value >>> 24) & 0xFF);
        arr[offset + 1] = (byte) ((value >>> 16) & 0xFF);
        arr[offset + 2] = (byte) ((value >>> 8) & 0xFF);
        arr[offset + 3] = (byte) ((value >>> 0) & 0xFF);
    }

    /**
     * Returns the long at the given offset of the byte array.
     */
    public static long getLong(byte[] arr, int offset) {
        long c1 = (((int) arr[offset + 0]) & 0x0FF);
        long c2 = (((int) arr[offset + 1]) & 0x0FF);
        long c3 = (((int) arr[offset + 2]) & 0x0FF);
        long c4 = (((int) arr[offset + 3]) & 0x0FF);
        long c5 = (((int) arr[offset + 4]) & 0x0FF);
        long c6 = (((int) arr[offset + 5]) & 0x0FF);
        long c7 = (((int) arr[offset + 6]) & 0x0FF);
        long c8 = (((int) arr[offset + 7]) & 0x0FF);

        return (c1 << 56) + (c2 << 48) + (c3 << 40) +
                (c4 << 32) + (c5 << 24) + (c6 << 16) + (c7 << 8) + (c8);
    }

    /**
     * Sets the long at the given offset of the byte array.
     */
    public static void setLong(long value, byte[] arr, int offset) {
        arr[offset + 0] = (byte) ((value >>> 56) & 0xFF);
        arr[offset + 1] = (byte) ((value >>> 48) & 0xFF);
        arr[offset + 2] = (byte) ((value >>> 40) & 0xFF);
        arr[offset + 3] = (byte) ((value >>> 32) & 0xFF);
        arr[offset + 4] = (byte) ((value >>> 24) & 0xFF);
        arr[offset + 5] = (byte) ((value >>> 16) & 0xFF);
        arr[offset + 6] = (byte) ((value >>> 8) & 0xFF);
        arr[offset + 7] = (byte) ((value >>> 0) & 0xFF);
    }


}
