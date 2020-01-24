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

package com.pony.database.global;

import java.io.Reader;

/**
 * An interface used by the engine to access and process strings.  This
 * interface allows us to access the contents of a string that may be
 * implemented in several different ways.  For example, a string may be
 * represented as a java.lang.String object in memeory, or it may be
 * represented as an ASCII sequence in a store.
 *
 * @author Tobias Downer
 */

public interface StringAccessor {

    /**
     * Returns the number of characters in the string.
     */
    int length();

    /**
     * Returns a Reader that allows the string to be read sequentually from
     * start to finish.
     */
    Reader getReader();

    /**
     * Returns this string as a java.lang.String object.  Some care may be
     * necessary with this call because a very large string will require a lot
     * space on the heap.
     */
    String toString();

}

