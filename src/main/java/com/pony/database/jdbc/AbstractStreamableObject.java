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

package com.pony.database.jdbc;

import java.io.*;
import java.sql.SQLException;

import com.pony.util.PagedInputStream;

/**
 * An abstract class that provides various convenience behaviour for
 * creating streamable java.sql.Blob and java.sql.Clob classes.  A streamable
 * object is typically a large object that can be fetched in separate pieces
 * from the server.  A streamable object only survives for as long as the
 * ResultSet that it is part of is open.
 *
 * @author Tobias Downer
 */

abstract class AbstractStreamableObject {

    /**
     * The MConnection object that this object was returned as part of the result
     * of.
     */
    protected final MConnection connection;

    /**
     * The result_id of the ResultSet this clob is from.
     */
    protected final int result_set_id;

    /**
     * The streamable object identifier.
     */
    private final long streamable_object_id;

    /**
     * The type of encoding of the stream.
     */
    private final byte type;

    /**
     * The size of the streamable object.
     */
    private final long size;

    /**
     * Constructor.
     */
    AbstractStreamableObject(MConnection connection, int result_set_id,
                             byte type, long streamable_object_id, long size) {
        this.connection = connection;
        this.result_set_id = result_set_id;
        this.type = type;
        this.streamable_object_id = streamable_object_id;
        this.size = size;
    }

    /**
     * Returns the streamable object identifier for referencing this streamable
     * object on the server.
     */
    protected long getStreamableId() {
        return streamable_object_id;
    }

    /**
     * Returns the encoding type of this object.
     */
    protected byte getType() {
        return type;
    }

    /**
     * Returns the number of bytes in this streamable object.  Note that this
     * may not represent the actual size of the object when it is decoded.  For
     * example, a Clob may be encoded as 2-byte per character (unicode) so the
     * actual length of the clob with be size / 2.
     */
    protected long rawSize() {
        return size;
    }


    // ---------- Inner classes ----------

    /**
     * An InputStream that is used to read the data from the streamable object as
     * a basic byte encoding.  This maintains an internal buffer.
     */
    class StreamableObjectInputStream extends PagedInputStream {

        /**
         * The default size of the buffer.
         */
        private final static int B_SIZE = 64 * 1024;

        /**
         * Construct the input stream.
         */
        public StreamableObjectInputStream(long in_size) {
            super(B_SIZE, in_size);
        }

        protected void readPageContent(byte[] buf, long pos, int length)
                throws IOException {
            try {
                // Request a part of the blob from the server
                StreamableObjectPart part = connection.requestStreamableObjectPart(
                        result_set_id, streamable_object_id, pos, length);
                System.arraycopy(part.getContents(), 0, buf, 0, length);
            } catch (SQLException e) {
                throw new IOException("SQL Error: " + e.getMessage());
            }
        }

    }

}

