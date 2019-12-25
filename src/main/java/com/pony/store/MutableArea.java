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

