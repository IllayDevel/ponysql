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

import java.io.*;

/**
 * This is a ByteArrayOutputStream that allows access to the underlying byte
 * array.  It can be instantiated, and then used over and over as a temporary
 * buffer between the writeTo methods and the underlying random access file
 * stream.
 * <p>
 * @author Tobias Downer
 */

public final class CellBufferOutputStream extends ByteArrayOutputStream {

    /**
     * The Constructor.
     */
    public CellBufferOutputStream(int length) {
        super(length);
    }

    /**
     * Returns the underlying stream you should not use the stream while you have
     * a handle on this reference.
     */
    public byte[] getByteArray() {
        return buf;
    }

    /**
     * Sets the pointer to specified point in the array.
     */
    public void seek(int pointer) {
        count = pointer;
    }

}
