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

import java.util.Date;

import com.pony.util.BigNumber;

import java.io.*;

/**
 * This object compliments ObjectTransfer and provides a method to translate
 * any object into a type the database engine can process.
 *
 * @author Tobias Downer
 */

public class ObjectTranslator {

    /**
     * Translates the given object to a type the database can process.
     */
    public static Object translate(Object ob) {
        if (ob == null) {
            return null;
        } else if (ob instanceof String) {
            return StringObject.fromString((String) ob);
        } else if (ob instanceof StringObject ||
                ob instanceof BigNumber ||
                ob instanceof Date ||
                ob instanceof ByteLongObject ||
                ob instanceof Boolean ||
                ob instanceof StreamableObject) {
            return ob;
        } else if (ob instanceof byte[]) {
            return new ByteLongObject((byte[]) ob);
        } else if (ob instanceof Serializable) {
            return serialize(ob);
        } else {
//      System.out.println("Ob is: (" + ob.getClass() + ") " + ob);
            throw new Error("Unable to translate object.  " +
                    "It is not a primitive type or serializable.");
        }
    }

    /**
     * Serializes the Java object to a ByteLongObject.
     */
    public static ByteLongObject serialize(Object ob) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream ob_out = new ObjectOutputStream(bout);
            ob_out.writeObject(ob);
            ob_out.close();
            return new ByteLongObject(bout.toByteArray());
        } catch (IOException e) {
            throw new Error("Serialization error: " + e.getMessage());
        }
    }

    /**
     * Deserializes a ByteLongObject to a Java object.
     */
    public static Object deserialize(ByteLongObject blob) {
        if (blob == null) {
            return null;
        } else {
            try {
                ByteArrayInputStream bin =
                        new ByteArrayInputStream(blob.getByteArray());
                ObjectInputStream ob_in = new ObjectInputStream(bin);
                Object ob = ob_in.readObject();
                ob_in.close();
                return ob;
            } catch (ClassNotFoundException e) {
                throw new Error("Class not found: " + e.getMessage());
            } catch (IOException e) {
                throw new Error("De-serialization error: " + e.getMessage());
            }
        }
    }

}
