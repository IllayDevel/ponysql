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

package com.pony.database.global;

import java.io.*;
import java.util.Date;

import com.pony.util.BigNumber;

/**
 * Provides static methods for transfering different types of objects over
 * a Data input/output stream.
 *
 * @author Tobias Downer
 */

public class ObjectTransfer {

    /**
     * Makes an estimate of the size of the object.  This is useful for making
     * a guess for how much this will take up.
     */
    public static int size(Object ob) throws IOException {
        if (ob == null) {
            return 9;
        } else if (ob instanceof StringObject) {
            return (ob.toString().length() * 2) + 9;
        } else if (ob instanceof BigNumber) {
            return 15 + 9;
        } else if (ob instanceof Date) {
            return 8 + 9;
        } else if (ob instanceof Boolean) {
            return 2 + 9;
        } else if (ob instanceof ByteLongObject) {
            return ((ByteLongObject) ob).length() + 9;
        } else if (ob instanceof StreamableObject) {
            return 5 + 9;
        } else {
            throw new IOException("Unrecognised type: " + ob.getClass());
        }
    }

    /**
     * Returns the exact size an object will take up when serialized.
     */
    public static int exactSize(Object ob) throws IOException {
        if (ob == null) {
            return 1;
        } else if (ob instanceof StringObject) {
            return (ob.toString().length() * 2) + 1 + 4;
        } else if (ob instanceof BigNumber) {
            BigNumber n = (BigNumber) ob;
            if (n.canBeRepresentedAsInt()) {
                return 4 + 1;
            } else if (n.canBeRepresentedAsLong()) {
                return 8 + 1;
            }
            byte[] buf = n.toByteArray();
            return buf.length + 1 + 1 + 4 + 4;
        } else if (ob instanceof Date) {
            return 8 + 1;
        } else if (ob instanceof Boolean) {
            return 1 + 1;
        } else if (ob instanceof ByteLongObject) {
            return ((ByteLongObject) ob).length() + 1 + 8;
        } else if (ob instanceof StreamableObject) {
            return 1 + 1 + 4;
        } else {
            throw new IOException("Unrecognised type: " + ob.getClass());
        }
    }

    /**
     * Writes an object to the data output stream.
     */
    public static void writeTo(DataOutput out, Object ob) throws IOException {
        if (ob == null) {
            out.writeByte(1);
        } else if (ob instanceof StringObject) {
            String str = ob.toString();

            // All strings send as char array,
            out.writeByte(18);
            out.writeInt(str.length());
            out.writeChars(str);

        } else if (ob instanceof BigNumber) {
            BigNumber n = (BigNumber) ob;
            if (n.canBeRepresentedAsInt()) {
                out.writeByte(24);
                out.writeInt(n.intValue());
            } else if (n.canBeRepresentedAsLong()) {
                out.writeByte(8);
                out.writeLong(n.longValue());
            } else {
                out.writeByte(7);
                out.writeByte(n.getState());
                out.writeInt(n.getScale());
                byte[] buf = n.toByteArray();
                out.writeInt(buf.length);
                out.write(buf);
            }

//      out.writeByte(6);
//      // NOTE: This method is only available in 1.2.  This needs to be
//      //   compatible with 1.1 so we use a slower method,
////      BigInteger unscaled_val = n.unscaledValue();
//      // NOTE: This can be swapped out eventually when we can guarentee
//      //   everything is 1.2 minimum.
//      BigInteger unscaled_val = n.movePointRight(n.scale()).toBigInteger();
//
//      byte[] buf = unscaled_val.toByteArray();
//      out.writeInt(buf.length);
//      out.write(buf);
        } else if (ob instanceof Date) {
            Date d = (Date) ob;
            out.writeByte(9);
            out.writeLong(d.getTime());
        } else if (ob instanceof Boolean) {
            Boolean b = (Boolean) ob;
            out.writeByte(12);
            out.writeBoolean(b);
        } else if (ob instanceof ByteLongObject) {
            ByteLongObject barr = (ByteLongObject) ob;
            out.writeByte(15);
            byte[] arr = barr.getByteArray();
            out.writeLong(arr.length);
            out.write(arr);
        } else if (ob instanceof StreamableObject) {
            StreamableObject ob_head = (StreamableObject) ob;
            out.writeByte(16);
            out.writeByte(ob_head.getType());
            out.writeLong(ob_head.getSize());
            out.writeLong(ob_head.getIdentifier());
        } else {
            throw new IOException("Unrecognised type: " + ob.getClass());
        }
    }

    /**
     * Writes an object from the data input stream.
     */
    public static Object readFrom(DataInputStream in) throws IOException {
        byte type = in.readByte();

        switch (type) {
            case (1):
                return null;

            case (3):
                String str = in.readUTF();
                return StringObject.fromString(str);

            case (6): {
                int scale = in.readInt();
                int blen = in.readInt();
                byte[] buf = new byte[blen];
                in.readFully(buf);
                return BigNumber.fromData(buf, scale, (byte) 0);
            }

            case (7): {
                byte state = in.readByte();
                int scale = in.readInt();
                int blen = in.readInt();
                byte[] buf = new byte[blen];
                in.readFully(buf);
                return BigNumber.fromData(buf, scale, state);
            }

            case (8): {
                // 64-bit long numeric value
                long val = in.readLong();
                return BigNumber.fromLong(val);
            }

            case (9):
                long time = in.readLong();
                return new Date(time);

            case (12):
                return in.readBoolean();

            case (15): {
                long size = in.readLong();
                byte[] arr = new byte[(int) size];
                in.readFully(arr, 0, (int) size);
                return new ByteLongObject(arr);
            }

            case (16): {
                final byte h_type = in.readByte();
                final long h_size = in.readLong();
                final long h_id = in.readLong();
                return new StreamableObject(h_type, h_size, h_id);
            }

            case (18): {
                // Handles strings > 64k
                int len = in.readInt();
                StringBuffer buf = new StringBuffer(len);
                while (len > 0) {
                    buf.append(in.readChar());
                    --len;
                }
                return StringObject.fromString(new String(buf));
            }

            case (24): {
                // 32-bit int numeric value
                long val = (long) in.readInt();
                return BigNumber.fromLong(val);
            }

            default:
                throw new IOException("Unrecognised type: " + type);

        }
    }

}
