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

package com.pony.util;

/**
 * A block of an AbstractBlockIntegerList.  This exposes the contents of a
 * block of the list.
 * <p>
 * An IntegerListBlockInterface is a single element of a block of integers
 * that make up some complete list of integers.  A block integer list
 * encapsulates a set of integers making up the block, and a chain to the
 * next and previous block in the set.
 *
 * @author Tobias Downer
 */

public abstract class IntegerListBlockInterface {

    /**
     * The next block in the chain.
     */
    public IntegerListBlockInterface next;

    /**
     * The previous block in the chain.
     */
    public IntegerListBlockInterface previous;

    /**
     * Set to true whenever the integers of this block are changed via the
     * mutation methods.
     */
    boolean has_changed;


    /**
     * Returns true if this store has been modified.  The purpose of this
     * method is to determine if any updates need to be made to any
     * persistant representation of this store.
     */
    public final boolean hasChanged() {
        return has_changed;
    }

    /**
     * Returns the number of entries in this block.
     */
    public abstract int size();

    /**
     * Returns true if the block is full.
     */
    public abstract boolean isFull();

    /**
     * Returns true if the block is empty.
     */
    public abstract boolean isEmpty();

    /**
     * Returns true if the block has enough room to fill with the given number
     * of integers.
     */
    public abstract boolean canContain(int number);

    /**
     * The top int in the list.
     */
    public abstract int topInt();

    /**
     * The bottom int in the list.
     */
    public abstract int bottomInt();

    /**
     * Returns the int at the given position in the array.
     */
    public abstract int intAt(int pos);

    /**
     * Adds an int to the block.
     */
    public abstract void addInt(int val);

    /**
     * Removes an Int from the specified position in the block.
     */
    public abstract int removeIntAt(int pos);

    /**
     * Inserts an int at the given position.
     */
    public abstract void insertIntAt(int val, int pos);

    /**
     * Sets an int at the given position, overwriting anything that was
     * previously there.  It returns the value that was previously at the
     * element.
     */
    public abstract int setIntAt(int val, int pos);

    /**
     * Moves a set of values from the end of this block and inserts it into the
     * given block at the destination index specified.  Assumes the
     * destination block has enough room to store the set.  Assumes
     * 'dest_block' is the same class as this.
     */
    public abstract void moveTo(IntegerListBlockInterface dest_block,
                                int dest_index, int length);

    /**
     * Copies all the data from this block into the given destination block.
     * Assumes 'dest_block' is the same class as this.
     */
    public abstract void copyTo(IntegerListBlockInterface dest_block);

    /**
     * Copies all the data from this block into the given int[] array.  Returns
     * the number of 'int' values copied.
     */
    public abstract int copyTo(int[] to, int offset);

    /**
     * Clears the object to be re-used.
     */
    public abstract void clear();

    /**
     * Performs an iterative search through the int values in the list.
     * If it's found the index of the value is returned, else it returns
     * -1.
     */
    public abstract int iterativeSearch(int val);

    /**
     * Performs an iterative search from the given position to the end of
     * the list in the block.
     * If it's found the index of the value is returned, else it returns
     * -1.
     */
    public abstract int iterativeSearch(int val, int position);


    // ---------- Sort algorithms ----------

    /**
     * Considers each int a reference to another structure, and the block
     * sorted by these structures.  The method performs a binary search.
     */
    public abstract int binarySearch(Object key, IndexComparator c);

    /**
     * Considers each int a reference to another structure, and the block
     * sorted by these structures.  Finds the first index in the block that
     * equals the given key.
     */
    public abstract int searchFirst(Object key, IndexComparator c);

    /**
     * Considers each int a reference to another structure, and the block
     * sorted by these structures.  Finds the first index in the block that
     * equals the given key.
     */
    public abstract int searchLast(Object key, IndexComparator c);

    /**
     * Assuming a sorted block, finds the first index in the block that
     * equals the given value.
     */
    public abstract int searchFirst(int val);

    /**
     * Assuming a sorted block, finds the first index in the block that
     * equals the given value.
     */
    public abstract int searchLast(int val);

}
