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

import java.io.IOException;

/**
 * An interface that represents a reference to a object that isn't stored in
 * main memory.  The reference to the object is made through the id value
 * returned by the 'getID' method.
 *
 * @author Tobias Downer
 */

public interface Ref {

    /**
     * An id used to reference this object in the context of the database.  Note
     * that once a static reference is made (or removed) to/from this object, the
     * BlobStore should be notified of the reference.  The store will remove an
     * large object that has no references to it.
     */
    long getID();

    /**
     * The type of large object that is being referenced.  2 = binary object,
     * 3 = ASCII character object, 4 = Unicode character object.
     */
    byte getType();

    /**
     * The 'raw' size of this large object in bytes when it is in its byte[]
     * form.  This value allows us to know how many bytes we can read from this
     * large object when it's being transferred to the client.
     */
    long getRawSize();

    /**
     * Reads a part of this large object from the store into the given byte
     * buffer.  This method should only be used when reading a large object
     * to transfer to the JDBC driver.  It represents the byte[] representation
     * of the object only and is only useful for transferral of the large object.
     */
    void read(long offset, byte[] buf, int length) throws IOException;

    /**
     * This method is used to write the contents of the large object into the
     * backing store.  This method will only work when the large object is in
     * an initial 'write' phase in which the client is pushing the contents of
     * the large object onto the server to be stored.
     */
    void write(long offset, byte[] buf, int length) throws IOException;

    /**
     * This method is called when the write phrase has completed, and it marks
     * this large object as complete.  After this method is called the large
     * object reference is a static object that can not be changed.
     */
    void complete() throws IOException;

}

