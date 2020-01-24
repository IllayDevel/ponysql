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

import java.io.InputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.Reader;

/**
 * An object that wraps around a Reader and translates the unicode stream into
 * a stream of bytes that the database is able to transfer to the database.
 * This object simply converts each char from the Reader into two bytes.  See
 * also BinaryToUnicodeReader for the Reader version of this class.
 *
 * @author Tobias Downer
 */

final class UnicodeToBinaryStream extends InputStream {

    /**
     * The Reader we are wrapping.
     */
    private final Reader reader;

    /**
     * If this is 0 we are on the left byte of the character.  If this is 1 we
     * are on the right byte of the current character.
     */
    private int lr_byte;

    /**
     * The current character if 'lr_byte' is 1.
     */
    private int current_c;

    /**
     * Constructs the stream.
     */
    public UnicodeToBinaryStream(Reader reader) {
        // Note, we wrap the input Reader around a BufferedReader.
        // This is a bit lazy.  Perhaps a better implementation of this would
        // implement 'read(byte[] buf, ...)' and provide its own buffering.
        this.reader = new BufferedReader(reader);
        lr_byte = 0;
    }

    /**
     * Reads the next character from the stream.
     */
    public int read() throws IOException {
        if (lr_byte == 0) {
            current_c = reader.read();
            if (current_c == -1) {
                return -1;
            }
            lr_byte = 1;
            return (current_c >> 8) & 0x0FF;
        } else {
            lr_byte = 0;
            return current_c & 0x0FF;
        }
    }

    public int available() throws IOException {
        return 0;
    }

}

