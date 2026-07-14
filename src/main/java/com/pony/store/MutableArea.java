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

package com.pony.store;

import java.io.IOException;

/**
 * An interface for an area that can be modified.  Any changes made to an area
 * may or may not be immediately reflected in already open areas with the same
 * id.  The specification does guarentee that after the 'checkOutAndClose'
 * method is invoked that any new Area or MutableArea objects created by the
 * backing store will contain the changes.
 *
 * @author Tobias Downer
 */

public interface MutableArea extends Area {

    /**
     * Checks out all changes made to this area.  This should be called after a
     * series of updates have been made to the area and the final change is to
     * be 'finalized'.  When this method returns, any new Area or MutableArea
     * objects created by the backing store will contain the changes made to this
     * object.  Any changes made to the Area may or may not be made to any
     * already existing areas.
     * <p>
     * In a logging implementation, this may flush out the changes made to the
     * area in a log.
     */
    void checkOut() throws IOException;

    // ---------- Various put methods ----------

    void put(byte b) throws IOException;

    void put(byte[] buf, int off, int len) throws IOException;

    void put(byte[] buf) throws IOException;

    void putShort(short s) throws IOException;

    void putInt(int i) throws IOException;

    void putLong(long l) throws IOException;

    void putChar(char c) throws IOException;

}

