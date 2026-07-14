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

