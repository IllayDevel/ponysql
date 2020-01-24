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

import java.util.List;
import java.io.InputStream;
import java.io.IOException;

/**
 * A store is a resource where areas can be allocated and freed to store
 * objects.  A store can be backed by a file or main memory, or a combination of
 * the two.
 * <p>
 * Some characteristics of implementations of Store may be separately
 * specified.  For example, a file based store that is intended to persistently
 * store objects may have robustness as a primary requirement.  A main memory
 * based store, or other type of volatile store, may not require robustness.
 *
 * @author Tobias Downer
 */

public interface Store {

    /**
     * Allocates a block of memory in the store of the specified size and returns
     * an AreaWriter object that can be used to initialize the contents of the
     * area.  Note that an area in the store is undefined until the 'finish'
     * method is called in AreaWriter.
     *
     * @param size the amount of memory to allocate.
     * @return an AreaWriter object that allows the area to be setup.
     * @throws IOException if not enough space available to create the area or
     *   the store is read-only.
     */
    AreaWriter createArea(long size) throws IOException;

    /**
     * Deletes an area that was previously allocated by the 'createArea' method
     * by the area id.  Once an area is deleted the resources may be reclaimed.
     * The behaviour of this method is undefined if the id doesn't represent a
     * valid area.
     *
     * @param id the identifier of the area to delete.
     * @throws IOException (optional) if the id is invalid or the area can not
     *   otherwise by deleted.
     */
    void deleteArea(long id) throws IOException;

    /**
     * Returns an InputStream implementation that allows for the area with the
     * given identifier to be read sequentially.  The behaviour of this method,
     * and InputStream object, is undefined if the id doesn't represent a valid
     * area.
     * <p>
     * When 'id' is -1 then a fixed area (64 bytes in size) in the store is
     * returned.  The fixed area can be used to store important static
     * static information.
     *
     * @param id the identifier of the area to read, or id = -1 is a 64 byte
     *   fixed area in the store.
     * @return an InputStream that allows the area to be read from the start.
     * @throws IOException (optional) if the id is invalid or the area can not
     *   otherwise be accessed.
     */
    InputStream getAreaInputStream(long id) throws IOException;

    /**
     * Returns an object that allows for the contents of an area (represented by
     * the 'id' parameter) to be read.  The behaviour of this method, and Area
     * object, is undefined if the id doesn't represent a valid area.
     * <p>
     * When 'id' is -1 then a fixed area (64 bytes in size) in the store is
     * returned.  The fixed area can be used to store important static
     * static information.
     *
     * @param id the identifier of the area to read, or id = -1 is a 64 byte
     *   fixed area in the store.
     * @return an Area object that allows access to the part of the store.
     * @throws IOException (optional) if the id is invalid or the area can not
     *   otherwise be accessed.
     */
    Area getArea(long id) throws IOException;

    /**
     * Returns an object that allows for the contents of an area (represented by
     * the 'id' parameter) to be read and written.  The behaviour of this method,
     * and MutableArea object, is undefined if the id doesn't represent a valid
     * area.
     * <p>
     * When 'id' is -1 then a fixed area (64 bytes in size) in the store is
     * returned.  The fixed area can be used to store important static
     * static information.
     *
     * @param id the identifier of the area to access, or id = -1 is a 64 byte
     *   fixed area in the store.
     * @return a MutableArea object that allows access to the part of the store.
     * @throws IOException (optional) if the id is invalid or the area can not
     *   otherwise be accessed.
     */
    MutableArea getMutableArea(long id) throws IOException;

    // ---------- Check Point Locking ----------

    /**
     * It is often useful to guarentee that a certain sequence of updates to a
     * store is completed and not broken in the middle.  For example, when
     * inserting data into a table you don't want a record to be partially
     * written when a check point is made.  You want the entire sequence of
     * modifications to be completed before the check point can run.  This
     * means that if a crash occurs, a check point will not recover to a
     * possible corrupt file.
     * <p>
     * To achieve this, the 'lockForWrite' and 'unlockForWrite' methods are
     * available.  When 'lockForWrite' has been called, a check point can not
     * created until there are no write locks obtained on the table.
     */
    void lockForWrite();

    /**
     * See the 'lockForWrite' method description.
     */
    void unlockForWrite();

    // ---------- Diagnostic ----------

    /**
     * Returns true if the store was closed cleanly.  This is important
     * information that may need to be considered when reading information from
     * the store.  This is typically used to issue a scan on the data in the
     * store when it is not closed cleanly.
     */
    boolean lastCloseClean();

    /**
     * Returns a complete list of pointers to all areas in the Store as Long
     * objects sorted from lowest pointer to highest.  This should be used for
     * diagnostics only because it may be difficult for this to be generated
     * with some implementations.  It is useful in a repair tool to determine if
     * a pointer is valid or not.
     */
    List getAllAreas() throws IOException;

}

