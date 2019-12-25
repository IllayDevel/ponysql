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

package com.pony.store;

import java.io.IOException;

/**
 * An interface that allows for the reading and writing of pages to/from a
 * journalled.
 *
 * @author Tobias Downer
 */

interface JournalledResource {

    /**
     * Returns the page size.
     */
    int getPageSize();

    /**
     * Returns a unique id for this resource.
     */
    long getID();

    /**
     * Reads a page of some previously specified size into the byte array.
     */
    void read(long page_number, byte[] buf, int off) throws IOException;

    /**
     * Writes a page of some previously specified size to the top log.  This will
     * add a single entry to the log and any 'read' operations after will contain
     * the written data.
     */
    void write(long page_number, byte[] buf, int off, int len) throws IOException;

    /**
     * Sets the new size of the resource.  This will add a single entry to the
     * log.
     */
    void setSize(long size) throws IOException;

    /**
     * Returns the current size of this resource.
     */
    long getSize() throws IOException;

    /**
     * Opens the resource.
     */
    void open(boolean read_only) throws IOException;

    /**
     * Closes the resource.  This will actually simply log that the resource has
     * been closed.
     */
    void close() throws IOException;

    /**
     * Deletes the resource.  This will actually simply log that the resource has
     * been deleted.
     */
    void delete() throws IOException;

    /**
     * Returns true if the resource currently exists.
     */
    boolean exists();

}

