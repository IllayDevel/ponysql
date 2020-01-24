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

package com.pony.debug;

/**
 * An interface for logging errors, warnings, messages, and exceptions in the
 * Pony system.  The implementation of where the log is written (to the
 * console, file, window, etc) is implementation defined.
 *
 * @author Tobias Downer
 */

public interface DebugLogger extends Lvl {

    /**
     * Queries the current debug level.  Returns true if the debug listener is
     * interested in debug information of this given level.  This can be used to
     * speed up certain complex debug displaying operations where the debug
     * listener isn't interested in the information be presented.
     */
    boolean isInterestedIn(int level);

    /**
     * This writes the given debugging string.  It filters out any messages that
     * are below the 'debug_level' variable.  The 'object' variable specifies
     * the object that made the call.  'level' must be between 0 and 255.  A
     * message of 'level' 255 will always print.
     */
    void write(int level, Object ob, String message);

    void write(int level, Class<?> cla, String message);

    void write(int level, String class_string, String message);

    /**
     * This writes the given Exception.  Exceptions are always output to the log
     * stream.
     */
    void writeException(Throwable e);

    /**
     * This writes the given Exception but gives it a 'debug_level'.  This is
     * so we can write out a warning exception.
     */
    void writeException(int level, Throwable e);

}
