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
