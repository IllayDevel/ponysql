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

package com.pony.store;

import java.io.IOException;

/**
 * An interface for access the contents of an area of a store.  The area object
 * maintains a pointer that can be manipulated and read from.
 *
 * @author Tobias Downer
 */

public interface Area {

    /**
     * Returns the unique identifier that represents this area.
     */
    long getID();

    /**
     * Returns the current position of the pointer within the area.  The position
     * starts at beginning of the area.
     */
    int position();

    /**
     * Returns the capacity of the area.
     */
    int capacity();

    /**
     * Sets the position within the area.
     */
    void position(int position) throws IOException;

    /**
     * Copies 'size' bytes from the current position of this Area to the
     * destination AreaWriter.
     */
    void copyTo(AreaWriter destination_writer, int size) throws IOException;

    // ---------- The get methods ----------
    // Note that these methods will all increment the position by the size of the
    // element read.  For example, 'getInt' will increment the position by 4.

    byte get() throws IOException;

    void get(byte[] buf, int off, int len) throws IOException;

    short getShort() throws IOException;

    int getInt() throws IOException;

    long getLong() throws IOException;

    char getChar() throws IOException;

}

