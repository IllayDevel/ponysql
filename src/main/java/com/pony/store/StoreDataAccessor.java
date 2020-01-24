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
 * An interface for low level store data access methods.  This is used to
 * implement a variety of ways of accessing data from some resource, such as
 * a file in a filesystem.  For example, we might use this to access a file
 * using the NIO API, or through the IO API.  Alternatively we may use it to
 * implement a scattering store that includes data across multiple files in the
 * filesystem.
 *
 * @author Tobias Downer
 */

interface StoreDataAccessor {

    /**
     * Returns true if the resource exists.
     */
    boolean exists();

    /**
     * Deletes the data area resource.  Returns true if the delete was successful.
     */
    boolean delete();

    /**
     * Opens the underlying data area representation.  If the resource doesn't
     * exist then it is created and the size is set to 0.
     */
    void open(boolean read_only) throws IOException;

    /**
     * Closes the underlying data area representation.
     */
    void close() throws IOException;


    /**
     * Reads a block of data from the underlying data area at the given position
     * into the byte array at the given offset.
     */
    void read(long position, byte[] buf, int off, int len) throws IOException;

    /**
     * Writes a block of data to the underlying data area from the byte array at
     * the given offset.
     */
    void write(long position, byte[] buf, int off, int len) throws IOException;

    /**
     * Sets the size of the underlying data area to the given size.  If the size
     * of the data area is increased, the content between the old size and the
     * new size is implementation defined.
     */
    void setSize(long new_size) throws IOException;

    /**
     * Returns the current size of the underlying data area.
     */
    long getSize() throws IOException;

    /**
     * Synchronizes the data area by forcing any data out of the OS buffers onto
     * the disk.
     */
    void synch() throws IOException;


}

