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

/**
 * An object that is streamable (such as a long binary object, or
 * a long string object).  This is passed between client and server and
 * contains basic primitive information about the object it represents.  The
 * actual contents of the object itself must be obtained through other
 * means (see com.pony.database.jdbc.DatabaseInterface).
 *
 * @author Tobias Downer
 */

public final class StreamableObject {

    /**
     * The type of the object.
     */
    private byte type;

    /**
     * The size of the object in bytes.
     */
    private long size;

    /**
     * The identifier that identifies this object.
     */
    private long id;

    /**
     * Constructs the StreamableObject.
     */
    public StreamableObject(byte type, long size, long id) {
        this.type = type;
        this.size = size;
        this.id = id;
    }

    /**
     * Returns the type of object this stub represents.  Returns 1 if it
     * represents 2-byte unicde character object, 2 if it represents binary data.
     */
    public byte getType() {
        return type;
    }

    /**
     * Returns the size of the object stream, or -1 if the size is unknown.  If
     * this represents a unicode character string, you would calculate the total
     * characters as size / 2.
     */
    public long getSize() {
        return size;
    }

    /**
     * Returns an identifier that can identify this object within some context.
     * For example, if this is a streamable object on the client side, then the
     * identifier might be the value that is able to retreive a section of the
     * streamable object from the DatabaseInterface.
     */
    public long getIdentifier() {
        return id;
    }

}

