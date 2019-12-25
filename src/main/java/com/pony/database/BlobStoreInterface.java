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

import java.io.IOException;

import com.pony.database.global.Ref;

/**
 * A very restricted interface for accessing a blob store.  This is used by a
 * MasterTableDataSource implementation to query and resolve blob information.
 *
 * @author Tobias Downer
 */

public interface BlobStoreInterface {

    /**
     * Given a large object reference identifier, generates a Ref implementation
     * that provides access to the information in the large object.  The Ref
     * implementation returned by this object is a read-only static object.
     * This may return either a BlobRef or a ClobRef object depending on the
     * type of the object.
     */
    Ref getLargeObject(long reference_id) throws IOException;

    /**
     * Tells the BlobStore that a static reference has been established in a
     * table to the blob referenced by the given id.  This is used to count
     * references to a blob, and possibly clean up a blob if there are no
     * references remaining to it.
     */
    void establishReference(long reference_id);

    /**
     * Tells the BlobStore that a static reference has been released to the
     * given blob.  This would typically be called when the row in the database
     * is removed.
     */
    void releaseReference(long reference_id);

}

