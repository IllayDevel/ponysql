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

package com.pony.database.jdbc;

/**
 * Represents a response from the server for a section of a streamable object.
 * A streamable object can always be represented as a byte[] array and is
 * limited to String (as 2-byte unicode) and binary data types.
 *
 * @author Tobias Downer
 */

public class StreamableObjectPart {

    /**
     * The byte[] array that is the contents of the cell from the server.
     */
    private byte[] part_contents;

    /**
     * Constructs the ResultCellPart.  Note that the 'contents' byte array must
     * be immutable.
     */
    public StreamableObjectPart(byte[] contents) {
        this.part_contents = contents;
    }

    /**
     * Returns the contents of this ResultCellPart.
     */
    public byte[] getContents() {
        return part_contents;
    }

}

