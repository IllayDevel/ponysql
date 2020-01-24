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

import java.io.OutputStream;
import java.io.IOException;

/**
 * The interface used for setting up an area initially in a store.  This method
 * is intended to optimize the area creation process.  Typically an area is
 * created at a specified size and filled with data.  This area should be
 * used as follows;
 * <p><pre>
 *    AreaWriter writer = store.createArea(16);
 *    writer.putInt(3);
 *    writer.putLong(100030);
 *    writer.putByte(1);
 *    writer.putShort(0);
 *    writer.putByte(2);
 *    writer.finish();
 * </pre></p>
 * When the 'finish' method is called, the AreaWriter object is invalidated and
 * the area can then be accessed in the store by the 'getArea' method.
 * <p>
 * Note that an area may only be written sequentially using this object.  This
 * is by design and allows for the area initialization process to be optimized.
 *
 * @author Tobias Downer
 */

public interface AreaWriter {

    /**
     * Returns the unique identifier that represents this area in the store.
     */
    long getID();

    /**
     * Returns an OutputStream that can be used to write to this area.  This
     * stream is backed by this area writer, so if 10 bytes area written to the
     * output stream then the writer position is also incremented by 10 bytes.
     */
    OutputStream getOutputStream();

    /**
     * Returns the size of this area.
     */
    int capacity();

    /**
     * Finishes the area writer object.  This must be called when the area is
     * completely initialized.  After this method is called the object is
     * invalidated and the area can be accessed in the store.
     */
    void finish() throws IOException;

    // ---------- Various put methods ----------

    void put(byte b) throws IOException;

    void put(byte[] buf, int off, int len) throws IOException;

    void put(byte[] buf) throws IOException;

    void putShort(short s) throws IOException;

    void putInt(int i) throws IOException;

    void putLong(long l) throws IOException;

    void putChar(char c) throws IOException;

}

