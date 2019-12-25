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
 * An interface for querying and accessing a list of primitive integers.  The
 * list may or may not be sorted or may be sorted over an IndexComparator.
 * This interface exposes general list querying/inserting/removing methods.
 * <p>
 * How the list is physically stored is dependant on the implementation of
 * the interface.  An example of an implementation is 'BlockIntegerList'.
 *
 * @author Tobias Downer
 */

public interface IntegerListInterface {

    /**
     * Makes this list immutable effectively making it read-only.  After this
     * method, any calls to methods that modify the list will throw an error.
     * <p>
     * Once 'setImmutable' is called, the list can not be changed back to
     * being mutable.
     */
    void setImmutable();

    /**
     * Returns true if this interface is immutable.
     */
    boolean isImmutable();

    /**
     * The number of integers that are in the list.
     */
    int size();

    /**
     * Returns the int at the given position (0 first, 1 second, etc) in the
     * list.  If the position is out of bounds an exception is thrown.
     */
    int get(int pos);

    /**
     * Adds an integet to the given position in the list.  If the position is
     * out of bounds an exception is thrown.  Any values after the given
     * position are shifted forward.
     */
    void add(int val, int pos);

    /**
     * Adds an int to the end of the list.
     */
    void add(int val);

    /**
     * Removes an int from the given position in the list.  Returns the value
     * that was removed from the removed position.  If the position is out of
     * bounds an exception is thrown.
     */
    int remove(int pos);

    /**
     * Assuming the list is sorted, this performs a binary search and returns
     * true if the value is found, otherwise returns false.  If the list is not
     * sorted then this may return false even if the list does contain the
     * value.
     */
    boolean contains(int val);

    /**
     * Inserts plain 'int' values into the list in sorted order.
     */
    void insertSort(int val);

    /**
     * Inserts plain 'int' value into the sorted position in the list only if
     * it isn't already in the list.  If the value is inserted it returns true,
     * otherwise if the value wasn't inserted because it's already in the list,
     * it returns false.
     */
    boolean uniqueInsertSort(int val);

    /**
     * Removes a plain 'int' value from the sorted position in the list only if
     * it's already in the list.  If the value is removed it returns true,
     * otherwise if the value wasn't removed because it couldn't be found in the
     * list, it returns false.
     */
    boolean removeSort(int val);

    // ---------- IndexComparator methods ----------
    // NOTE: The IndexComparator methods offer the ability to maintain a set
    //  of index values that reference complex objects.  This is used to manage a
    //  sorted list of integers by their referenced object instead of the int
    //  value itself.  This enables us to create a vaste list of indexes without
    //  having to store the list of objects in memory.

    /**
     * Assuming the list is sorted, this performs a binary search and returns
     * true if the key value is found, otherwise returns false.
     */
    boolean contains(Object key, IndexComparator c);

    /**
     * Inserts the key/index pair into the list at the correct sorted position
     * (determine by the IndexComparator).  If the list already contains
     * identical key then the value is add to the end of the set of identical
     * values in the list.  This way, the sort is stable (the order of identical
     * elements does not change).
     */
    void insertSort(Object key, int val, IndexComparator c);

    /**
     * Removes the key/val pair from the list by first searching for it, and then
     * removing it from the list.  This method uses the IndexComparator object to
     * compare an index position in the list to an object to compare against.
     */
    int removeSort(Object key, int val, IndexComparator c);

    /**
     * Returns the index of the last value in this set that equals the given
     * value.  This method uses the IndexComparator object to compare an
     * index position in the list to an object to compare against.
     */
    int searchLast(Object key, IndexComparator c);

    /**
     * Returns the index of the first value in this set that equals the given
     * value.  This method uses the IndexComparator object to compare an
     * index position in the list to an object to compare against.
     */
    int searchFirst(Object key, IndexComparator c);

    // ---------- IntegerIterator methods ----------

    /**
     * Returns an IntegerIterator that will walk from the start offset
     * (inclusive) to the end offset (inclusive) of this list.
     */
    IntegerIterator iterator(int start_offset, int end_offset);

    /**
     * Returns an IntegerIterator that will walk from the start to the end
     * this list.
     */
    IntegerIterator iterator();

}
