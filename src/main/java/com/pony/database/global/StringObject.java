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

package com.pony.database.global;

import java.io.Reader;
import java.io.StringReader;

/**
 * A concrete implementation of StringAccessor that uses a java.lang.String
 * object.
 *
 * @author Tobias Downer
 */

public class StringObject implements java.io.Serializable, StringAccessor {

    static final long serialVersionUID = 6066215992031250481L;

    /**
     * The java.lang.String object.
     */
    private final String str;

    /**
     * Constructs the object.
     */
    private StringObject(String str) {
        this.str = str;
    }

    /**
     * Returns the length of the string.
     */
    public int length() {
        return str.length();
    }

    /**
     * Returns a Reader that can read from the string.
     */
    public Reader getReader() {
        return new StringReader(str);
    }

    /**
     * Returns this object as a java.lang.String object (easy!)
     */
    public String toString() {
        return str;
    }

    /**
     * Static method that returns a StringObject from the given java.lang.String.
     */
    public static StringObject fromString(String str) {
        if (str != null) {
            return new StringObject(str);
        }
        return null;
    }

}

