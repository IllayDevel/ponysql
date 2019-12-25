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

import com.pony.util.ByteArrayUtil;

import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

/**
 * An implementation of the Store interface that persists information in the
 * volatile JVM heap memory.  Each Area in the store is represented by a byte[]
 * array from the Java heap.
 * <p>
 * Note that in Java there is no way to cast a reference to a numeric value,
 * or to cast a numeric value back into a reference.  This means that
 * Area lookup has to be coordinated via a hashing algorithm over an array.
 * There would prehaps be a more efficient way to achieve this with JNI but
 * it would mean we can't be pure Java and it would require locking the address
 * of objects in the heap.
 * <p>
 * Another alternative way of implementing this class would be to use JNI to
 * access a C style 'malloc' function in the operating system and wrap the
 * memory with a native Area implementation.
 *
 * @author Tobias Downer
 */

public final class HeapStore implements Store {

    /**
     * The fixed area element (a 64 byte area).
     */
    private HeapAreaElement fixed_area_element;

    /**
     * A hash map of area pointer to byte[] array that represents the area.
     */
    private HeapAreaElement[] area_map;

    /**
     * A unique id key incremented for each new area created.
     */
    private long unique_id_key;

    /**
     * Creates the HeapStore.
     */
    public HeapStore(int hash_size) {
        area_map = new HeapAreaElement[hash_size];
        unique_id_key = 0;
    }

    /**
     * Defaults heap size to 257 elements.
     */
    public HeapStore() {
        this(257);
    }

    /**
     * Searches the hash map and returns the area element for the given pointer.
     */
    private HeapAreaElement getAreaElement(long pointer) throws IOException {
        synchronized (this) {
            // Find the pointer in the hash
            int hash_pos = (int) (pointer % area_map.length);
            HeapAreaElement prev = null;
            HeapAreaElement element = area_map[hash_pos];
            // Search for this pointer
            while (element != null && element.getID() != pointer) {
                prev = element;
                element = element.next_hash_element;
            }
            // If not found
            if (element == null) {
                throw new IOException("Pointer " + pointer + " is invalid.");
            }
            // Move the element to the start of the list.
            if (prev != null) {
                prev.next_hash_element = element.next_hash_element;
                element.next_hash_element = area_map[hash_pos];
                area_map[hash_pos] = element;
            }
            // Return the element
            return element;
        }
    }

    /**
     * Returns a MutableArea object for the fixed position.
     */
    private HeapAreaElement getFixedAreaElement() {
        synchronized (this) {
            if (fixed_area_element == null) {
                fixed_area_element = new HeapAreaElement(-1, 64);
            }
            return fixed_area_element;
        }
    }

    /**
     * Returns the HeapAreaElement for the given pointer.
     */
    private HeapAreaElement getElement(long pointer) throws IOException {
        if (pointer == -1) {
            return getFixedAreaElement();
        } else {
            return getAreaElement(pointer);
        }
    }


    // ---------- Implemented from Store ----------

    public AreaWriter createArea(long size) throws IOException {
        if (size > Integer.MAX_VALUE) {
            throw new IOException("'size' is too large.");
        }
        synchronized (this) {
            // Generate a unique id for this area.
            long id = unique_id_key;
            ++unique_id_key;

            // Create the element.
            HeapAreaElement element = new HeapAreaElement(id, (int) size);
            // The position in the hash map
            int hash_pos = (int) (id % area_map.length);
            // Add to the chain
            element.next_hash_element = area_map[hash_pos];
            // Set the element in the chain
            area_map[hash_pos] = element;
            // And return the object
            return element.getAreaWriter();
        }
    }

    public void deleteArea(long pointer) throws IOException {
        synchronized (this) {
            // Find the pointer in the hash
            int hash_pos = (int) (pointer % area_map.length);
            HeapAreaElement prev = null;
            HeapAreaElement element = area_map[hash_pos];
            // Search for this pointer
            while (element != null && element.getID() != pointer) {
                prev = element;
                element = element.next_hash_element;
            }
            // If not found
            if (element == null) {
                throw new IOException("Pointer " + pointer + " is invalid.");
            }
            // Remove
            if (prev == null) {
                area_map[hash_pos] = element.next_hash_element;
            } else {
                prev.next_hash_element = element.next_hash_element;
            }
            // Garbage collector should do the rest...
        }
    }

    public InputStream getAreaInputStream(long pointer) throws IOException {
        return getElement(pointer).getInputStream();
    }

    public Area getArea(long pointer) throws IOException {
        return getElement(pointer).getMutableArea();
    }

    public MutableArea getMutableArea(long pointer) throws IOException {
        return getElement(pointer).getMutableArea();
    }

    public void flush() throws IOException {
        // Not required
    }

    public void synch() throws IOException {
        // Not required
    }

    public void lockForWrite() {
        // Not required
    }

    public void unlockForWrite() {
        // Not required
    }

    // ---------- Diagnostic ----------

    public boolean lastCloseClean() {
        // Close is not possible with a heap store, so always return true
        return true;
    }

    public List getAllAreas() throws IOException {
        throw new RuntimeException("PENDING");
    }


    // ---------- Inner classes ----------

    /**
     * An implementation of Area for a byte[] array from the heap.
     */
    private static class HeapArea implements MutableArea {

        /**
         * The ID of this area.
         */
        private final long id;

        /**
         * A pointer to the byte[] array representing the entire area.
         */
        private final byte[] heap_area;

        /**
         * The start pointer in the heap area.
         */
        private final int start_pointer;

        /**
         * The current pointer into the area.
         */
        private int position;

        /**
         * The end pointer of the area.
         */
        private final int end_pointer;

        /**
         * Constructor.
         */
        HeapArea(long id, byte[] heap_area, int offset, int length) {
            this.id = id;
            this.heap_area = heap_area;
            this.start_pointer = offset;
            this.position = offset;
            this.end_pointer = offset + length;
        }

        private int checkPositionBounds(int diff) throws IOException {
            final int new_pos = position + diff;
            if (new_pos > end_pointer) {
                throw new IOException("Position out of bounds. " +
                        " start=" + start_pointer +
                        " end=" + end_pointer +
                        " pos=" + position +
                        " new_pos=" + new_pos);
            }
            final int old_pos = position;
            position = new_pos;
            return old_pos;
        }

        public long getID() {
            return id;
        }

        public int position() {
            return position - start_pointer;
        }

        public int capacity() {
            return end_pointer - start_pointer;
        }

        public void position(int position) throws IOException {
            int act_position = start_pointer + position;
            if (act_position >= 0 && act_position < end_pointer) {
                this.position = act_position;
                return;
            }
            throw new IOException("Moved position out of bounds.");
        }

        public void copyTo(AreaWriter destination, int size) throws IOException {
            final int BUFFER_SIZE = 2048;
            byte[] buf = new byte[BUFFER_SIZE];
            int to_copy = Math.min(size, BUFFER_SIZE);

            while (to_copy > 0) {
                get(buf, 0, to_copy);
                destination.put(buf, 0, to_copy);
                size -= to_copy;
                to_copy = Math.min(size, BUFFER_SIZE);
            }
        }

        public byte get() throws IOException {
            return heap_area[checkPositionBounds(1)];
        }

        public void put(byte b) throws IOException {
            heap_area[checkPositionBounds(1)] = b;
        }

        public void get(byte[] buf, int off, int len) throws IOException {
            System.arraycopy(heap_area, checkPositionBounds(len), buf, off, len);
        }

        public void put(byte[] buf, int off, int len) throws IOException {
            System.arraycopy(buf, off, heap_area, checkPositionBounds(len), len);
        }

        public void put(byte[] buf) throws IOException {
            put(buf, 0, buf.length);
        }

        public short getShort() throws IOException {
            short s = ByteArrayUtil.getShort(heap_area, checkPositionBounds(2));
            return s;
        }

        public void putShort(short s) throws IOException {
            ByteArrayUtil.setShort(s, heap_area, checkPositionBounds(2));
        }

        public int getInt() throws IOException {
            int i = ByteArrayUtil.getInt(heap_area, checkPositionBounds(4));
            return i;
        }

        public void putInt(int i) throws IOException {
            ByteArrayUtil.setInt(i, heap_area, checkPositionBounds(4));
        }

        public long getLong() throws IOException {
            long l = ByteArrayUtil.getLong(heap_area, checkPositionBounds(8));
            return l;
        }

        public void putLong(long l) throws IOException {
            ByteArrayUtil.setLong(l, heap_area, checkPositionBounds(8));
        }

        public char getChar() throws IOException {
            char c = ByteArrayUtil.getChar(heap_area, checkPositionBounds(2));
            return c;
        }

        public void putChar(char c) throws IOException {
            ByteArrayUtil.setChar(c, heap_area, checkPositionBounds(2));
        }

        public void checkOut() {
            // no-op
        }

        public String toString() {
            return "[Area start_pointer=" + start_pointer +
                    " end_pointer=" + end_pointer +
                    " position=" + position + "]";
        }

    }

    private static class HeapAreaWriter extends HeapArea
            implements AreaWriter {

        public HeapAreaWriter(long id, byte[] heap_area, int offset, int length) {
            super(id, heap_area, offset, length);
        }

        public OutputStream getOutputStream() {
            return new AbstractStore.AreaOutputStream(this);
        }

        public void finish() throws IOException {
            // Currently, no-op
        }

    }

    /**
     * An area allocated from the heap store represented by a volatile byte[]
     * array.
     */
    private static class HeapAreaElement {

        /**
         * The id of this heap area (used as the hash key).
         */
        private final long heap_id;

        /**
         * A byte[] array that represents the volatile heap area.
         */
        private final byte[] heap_area;

        /**
         * The pointer to the next HeapAreaElement in this hash key.
         */
        HeapAreaElement next_hash_element;

        /**
         * Constructs the HeapAreaElement.
         */
        HeapAreaElement(long heap_id, int area_size) {
            this.heap_id = heap_id;
            this.heap_area = new byte[area_size];
        }

        /**
         * Returns the heap id for this element.
         */
        long getID() {
            return heap_id;
        }

        /**
         * Returns a new AreaWriter object for this element.
         */
        AreaWriter getAreaWriter() {
            return new HeapAreaWriter(getID(), heap_area, 0, heap_area.length);
        }

        /**
         * Returns a new MutableArea object for this element.
         */
        MutableArea getMutableArea() {
            return new HeapArea(getID(), heap_area, 0, heap_area.length);
        }

        /**
         * Returns a new InputStream that is used to read from the area.
         */
        InputStream getInputStream() {
            return new ByteArrayInputStream(heap_area);
        }

    }

}

