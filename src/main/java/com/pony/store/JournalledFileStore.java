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
 * An implementation of AbstractStore that persists to an underlying data
 * format via a robust journalling system that supports check point and crash
 * recovery.  Note that this object is a bridge between the Store API and the
 * journalled behaviour defined in LoggingBufferManager, JournalledSystem and
 * the StoreDataAccessor implementations.
 * <p>
 * Note that access to the resources is abstracted via a 'resource_name'
 * string.  The LoggingBufferManager object converts the resource name into a
 * concrete object that accesses the actual data.
 *
 * @author Tobias Downer
 */

public final class JournalledFileStore extends AbstractStore {

    /**
     * The name of the resource.
     */
    private final String resource_name;

    /**
     * The buffering strategy for accessing the data in an underlying file.
     */
    private final LoggingBufferManager buffer_manager;

    /**
     * The JournalledResource object that's used to journal all read/write
     * operations to the above 'store_accessor'.
     */
    private JournalledResource store_resource;


    /**
     * Constructs the ScatteringFileStore.
     */
    public JournalledFileStore(String resource_name,
                               LoggingBufferManager buffer_manager,
                               boolean read_only) {
        super(read_only);
        this.resource_name = resource_name;
        this.buffer_manager = buffer_manager;

        // Create the store resource object for this resource name
        this.store_resource = buffer_manager.createResource(resource_name);
    }


    // ---------- JournalledFileStore methods ----------

    /**
     * Deletes this store from the file system.  This operation should only be
     * used when the store is NOT open.
     */
    public boolean delete() throws IOException {
        store_resource.delete();
        return true;
    }

    /**
     * Returns true if this store exists in the file system.
     */
    public boolean exists() throws IOException {
        return store_resource.exists();
    }

    public void lockForWrite() {
        try {
            buffer_manager.lockForWrite();
        } catch (InterruptedException e) {
            throw new Error("Interrupted: " + e.getMessage());
        }
    }

    public void unlockForWrite() {
        buffer_manager.unlockForWrite();
    }

    // ---------- Implemented from AbstractStore ----------

    /**
     * Internally opens the backing area.  If 'read_only' is true then the
     * store is opened in read only mode.
     */
    protected void internalOpen(boolean read_only) throws IOException {
        store_resource.open(read_only);
    }

    /**
     * Internally closes the backing area.
     */
    protected void internalClose() throws IOException {
        buffer_manager.close(store_resource);
    }


    protected int readByteFrom(long position) throws IOException {
        return buffer_manager.readByteFrom(store_resource, position);
    }

    protected int readByteArrayFrom(long position,
                                    byte[] buf, int off, int len) throws IOException {
        return buffer_manager.readByteArrayFrom(store_resource,
                position, buf, off, len);
    }

    protected void writeByteTo(long position, int b) throws IOException {
        buffer_manager.writeByteTo(store_resource, position, b);
    }

    protected void writeByteArrayTo(long position,
                                    byte[] buf, int off, int len) throws IOException {
        buffer_manager.writeByteArrayTo(store_resource,
                position, buf, off, len);
    }

    protected long endOfDataAreaPointer() throws IOException {
        return buffer_manager.getDataAreaSize(store_resource);
    }

    protected void setDataAreaSize(long new_size) throws IOException {
        buffer_manager.setDataAreaSize(store_resource, new_size);
    }

    // For diagnosis

    public String toString() {
        return "[ JournalledFileStore: " + resource_name + " ]";
    }

}

