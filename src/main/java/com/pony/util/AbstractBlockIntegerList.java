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

import java.util.ArrayList;

/**
 * An implementation of a list of integer values that are stored across
 * an array of blocks.  This allows for quicker insertion and deletion of
 * integer values, including other memory saving benefits.
 * <p>
 * The class works as follows;<p>
 * <ol>
 * <li>The list can contain any number of 'int' values.
 * <li>Each value is stored within a block of int's.  A block is of finite
 *     size.
 * <li>When a block becomes full, int values are moved around until enough
 *     space is free.  This may be by inserting a new block or by shifting
 *     information from one block to another.
 * <li>When a block becomes empty, it is removed.
 * </ol>
 * The benefits of this system are that inserts and deletes are fast even
 * for very large lists.  There are no megabyte sized arraycopys.  Also,
 * the object could be extended to a version that pages un-used blocks to disk
 * thus saving precious system memory.
 * <p>
 * NOTE: The following methods are <b>NOT</b> optimal:
 *   get(pos), add(val, pos), remove(pos)
 * <p>
 * Specifically, they slow as 'pos' nears the end of large lists.
 * <p>
 * This type of structure is very fast for large sorted lists where values can
 * be inserted at any position within the list.  Care needs to be taken for
 * lists where values are inserted and removed constantly, because
 * fragmentation of the list blocks can occur.  For example, adding 60,000
 * random entries followed by removing 50,000 random entries will result in
 * many only partially filled blocks.  Since each block takes a constant
 * amount of memory, this may not be acceptable.
 *
 * @author Tobias Downer
 */

public abstract class AbstractBlockIntegerList
        implements IntegerListInterface {

//  /**
//   * The size of each integer block.  (multiply by 4 to get rough size how
//   * much each block takes up in memory).
//   */
//  private static final int BLOCK_SIZE = 512;   // (2k memory per block)

    /**
     * The list of blocks (objects in this list are of type
     * 'IntegerListBlockInterface'.
     */
    protected ArrayList<IntegerListBlockInterface> block_list = new ArrayList<>(10);

    /**
     * The total number of ints in the list.
     */
    private int count;

//  /**
//   * The size of the blocks.
//   */
//  protected int block_size;

    /**
     * If this is set to true, then the list is immutable (we are not permitted
     * to insert or remove integers from the list).
     */
    private boolean immutable;


    /**
     * Constructs the list.
     */
    public AbstractBlockIntegerList() {
        immutable = false;
        count = 0;
//    block_size = BLOCK_SIZE;

//    insertListBlock(0, newListBlock());

    }

    /**
     * Constructs the list from the given set of initial blocks.
     */
    public AbstractBlockIntegerList(IntegerListBlockInterface[] blocks) {
        this();
        for (IntegerListBlockInterface block : blocks) {
            block_list.add(block);
            count += block.size();
        }
    }

    /**
     * Constructs the list by copying the contents from an IntegerVector.
     */
    public AbstractBlockIntegerList(IntegerVector ivec) {
        this();

        int sz = ivec.size();
        for (int i = 0; i < sz; ++i) {
            add(ivec.intAt(i));
        }
    }

    /**
     * Copies the information from the given BlockIntegerList into a new
     * object and performs a deep clone of the information in this container.
     */
    public AbstractBlockIntegerList(IntegerListInterface i_list) {
        this();

        if (i_list instanceof AbstractBlockIntegerList) {
            // Optimization for when the input list is a BlockIntegerList
            AbstractBlockIntegerList in_list = (AbstractBlockIntegerList) i_list;

//      block_size = in_list.block_size;

            ArrayList<IntegerListBlockInterface> in_blocks = in_list.block_list;
            int in_blocks_count = in_blocks.size();
            // For each block in 'in_list'
            for (int i = 0; i < in_blocks_count; ++i) {
                // get the block.
                IntegerListBlockInterface block =
                        (IntegerListBlockInterface) in_blocks.get(i);
                // Insert a new block in this object.
                IntegerListBlockInterface dest_block =
                        insertListBlock(i, newListBlock());
                // Copy the contents of the source block to the new destination block.
                block.copyTo(dest_block);
            }

            // Set the size of the list
            count = i_list.size();            //count;

        } else {
            // The case when IntegerListInterface type is not known
            IntegerIterator i = i_list.iterator();
            while (i.hasNext()) {
                add(i.next());
            }
        }

        // If the given list is immutable then set this list to immutable
        if (i_list.isImmutable()) {
            setImmutable();
        }

    }


    // ---------- Block operations ----------

    /**
     * Creates a new ListBlock for the given implementation.
     */
    protected abstract IntegerListBlockInterface newListBlock();

    /**
     * Called when the class decides this ListBlock is no longer needed.
     * Provided as an event for derived classes to intercept.
     */
    protected void deleteListBlock(IntegerListBlockInterface list_block) {
    }

    /**
     * Copies the data from each block into the given int[] array.  The int[]
     * array must be big enough to fit all the data in this list.
     */
    final void copyToArray(int[] array, int offset, int length) {
        if (array.length >= length && (offset + length) <= size()) {
            for (Object o : block_list) {
                IntegerListBlockInterface block =
                        (IntegerListBlockInterface) o;
                offset += block.copyTo(array, offset);
            }
            return;
        }
        throw new Error("Size mismatch.");
    }


    /**
     * Inserts a ListBlock at the given block in the list of ListBlock's.
     */
    private IntegerListBlockInterface
    insertListBlock(int index, IntegerListBlockInterface list_block) {
        block_list.add(index, list_block);

        // Point to next in the list.
        if (index + 1 < block_list.size()) {
            IntegerListBlockInterface next_b =
                    (IntegerListBlockInterface) block_list.get(index + 1);
            list_block.next = next_b;
            next_b.previous = list_block;
        } else {
            list_block.next = null;
        }

        // Point to previous in the list.
        if (index > 0) {
            IntegerListBlockInterface previous_b =
                    (IntegerListBlockInterface) block_list.get(index - 1);
            list_block.previous = previous_b;
            previous_b.next = list_block;
        } else {
            list_block.previous = null;
        }

        return list_block;
    }

    /**
     * Removes a IntegerListBlockInterface from the given index in the list of
     * IntegerListBlockInterface's.
     */
    private void removeListBlock(int index) {
        // Alter linked list pointers.
        IntegerListBlockInterface new_prev = null;
        IntegerListBlockInterface new_next = null;
        if (index + 1 < block_list.size()) {
            new_next = (IntegerListBlockInterface) block_list.get(index + 1);
        }
        if (index > 0) {
            new_prev = (IntegerListBlockInterface) block_list.get(index - 1);
        }

        if (new_prev != null) {
            new_prev.next = new_next;
        }
        if (new_next != null) {
            new_next.previous = new_prev;
        }

        IntegerListBlockInterface been_removed =
                (IntegerListBlockInterface) block_list.remove(index);
        deleteListBlock(been_removed);
    }

    /**
     * Inserts a value in the given block position in the list.
     */
    private void insertIntoBlock(int val, int block_index,
                                 IntegerListBlockInterface block, int position) {
        block.insertIntAt(val, position);
        ++count;
        // Is the block full?
        if (block.isFull()) {
            // We need to move half of the data out of this block into either the
            // next block or create a new block to store it.

            // The size that we going to zap out of this block.
            int move_size = (block.size() / 7) - 1;

            // The block to move half the data from this block.
            IntegerListBlockInterface move_to;
            // Is there a next block?
            if (block_index < block_list.size() - 1) {
                IntegerListBlockInterface next_b =
                        (IntegerListBlockInterface) block_list.get(block_index + 1);
//      IntegerListBlockInterface next_b = block.next;
//      if (next_b != null) {
                // Yes, can this block contain half the values from this block?
                if (next_b.canContain(move_size)) {
                    move_to = next_b;
                } else {
                    // Can't contain so insert a new block.
                    move_to = insertListBlock(block_index + 1, newListBlock());
                }

            } else {
                // No next block so create a new block
                move_to = insertListBlock(block_index + 1, newListBlock());
            }

            // 'move_to' should be set to the block we are to use to move half the
            // data from this block.
            block.moveTo(move_to, 0, move_size);

        }
    }

    /**
     * Removes the value from the given position in the specified block.  It
     * returns the value that used to be at that position.
     */
    protected final int removeFromBlock(int block_index,
                                        IntegerListBlockInterface block,
                                        int position) {
        int old_value = block.removeIntAt(position);
        --count;
        // If we have emptied out this block, then we should remove it from the
        // list.
        if (block.isEmpty() && block_list.size() > 1) {
            removeListBlock(block_index);
        }

        return old_value;
    }

    /**
     * Uses a binary search algorithm to quickly determine the index of the
     * IntegerListBlockInterface within 'block_list' of the block that contains
     * the given key value using the IndexComparator as a lookup comparator.
     */
    private int findBlockContaining(Object key, IndexComparator c) {
        if (count == 0) {
            return -1;
        }

        int low = 0;
        int high = block_list.size() - 1;

        while (low <= high) {
            int mid = (low + high) / 2;
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(mid);

            // Is what we are searching for lower than the bottom value?
            if (c.compare(block.bottomInt(), key) > 0) {
                high = mid - 1;
            }
            // No, then is it greater than the highest value?
            else if (c.compare(block.topInt(), key) < 0) {
                low = mid + 1;
            }
            // Must be inside this block then!
            else {
                return mid;
            }
        }

//    System.out.println("RETURNING: " + low);

        return -(low + 1);  // key not found.
    }

    /**
     * Uses a binary search algorithm to quickly determine the index of the
     * IntegerListBlockInterface within 'block_list' of the block that contains
     * the given key value using the IndexComparator as a lookup comparator.
     */
    private int findLastBlock(Object key, IndexComparator c) {
        if (count == 0) {
            return -1;
        }

        int low = 0;
        int high = block_list.size() - 1;

        while (low <= high) {

            if (high - low <= 2) {
                for (int i = high; i >= low; --i) {
                    IntegerListBlockInterface block =
                            (IntegerListBlockInterface) block_list.get(i);
                    if (c.compare(block.bottomInt(), key) <= 0) {
                        if (c.compare(block.topInt(), key) >= 0) {
                            return i;
                        } else {
                            return -(i + 1) - 1;
                        }
                    }
                }
                return -(low + 1);
            }

            int mid = (low + high) / 2;
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(mid);

            // Is what we are searching for lower than the bottom value?
            if (c.compare(block.bottomInt(), key) > 0) {
                high = mid - 1;
            }
            // No, then is it greater than the highest value?
            else if (c.compare(block.topInt(), key) < 0) {
                low = mid + 1;
            }
            // Equal, so highest must be someplace between mid and high.
            else {
                low = mid;
                if (low == high) {
                    return low;
                }
            }
        }

        return -(low + 1);  // key not found.
    }

    /**
     * Uses a binary search algorithm to quickly determine the index of the
     * IntegerListBlockInterface within 'block_list' of the block that contains
     * the given key value using the IndexComparator as a lookup comparator.
     */
    private int findFirstBlock(Object key, IndexComparator c) {
        if (count == 0) {
            return -1;
        }

        int low = 0;
        int high = block_list.size() - 1;

        while (low <= high) {

            if (high - low <= 2) {
                for (int i = low; i <= high; ++i) {
                    IntegerListBlockInterface block =
                            (IntegerListBlockInterface) block_list.get(i);
                    if (c.compare(block.topInt(), key) >= 0) {
                        if (c.compare(block.bottomInt(), key) <= 0) {
                            return i;
                        } else {
                            return -(i + 1);
                        }
                    }
                }
                return -(high + 1) - 1;
            }

            int mid = (low + high) / 2;
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(mid);

            // Is what we are searching for lower than the bottom value?
            if (c.compare(block.bottomInt(), key) > 0) {
                high = mid - 1;
            }
            // No, then is it greater than the highest value?
            else if (c.compare(block.topInt(), key) < 0) {
                low = mid + 1;
            }
            // Equal, so highest must be someplace between mid and high.
            else {
                high = mid;
            }
        }

        return -(low + 1);  // key not found.
    }


    /**
     * Uses a binary search algorithm to quickly determine the index of the
     * IntegerListBlockInterface within 'block_list' of the block that contains
     * the given value.
     */
    private int findFirstBlock(int val) {
        if (count == 0) {
            return -1;
        }

        int low = 0;
        int high = block_list.size() - 1;

        while (low <= high) {

            if (high - low <= 2) {
                for (int i = low; i <= high; ++i) {
                    IntegerListBlockInterface block =
                            (IntegerListBlockInterface) block_list.get(i);
                    if (block.topInt() >= val) {
                        if (block.bottomInt() <= val) {
                            return i;
                        } else {
                            return -(i + 1);
                        }
                    }
                }
                return -(high + 1) - 1;
            }

            int mid = (low + high) / 2;
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(mid);

            // Is what we are searching for lower than the bottom value?
            if (block.bottomInt() > val) {
                high = mid - 1;
            }
            // No, then is it greater than the highest value?
            else if (block.topInt() < val) {
                low = mid + 1;
            }
            // Equal, so highest must be someplace between mid and high.
            else {
                high = mid;
            }
        }

        return -(low + 1);  // key not found.
    }

    /**
     * Uses a binary search algorithm to quickly determine the index of the
     * IntegerListBlockInterface within 'block_list' of the block that contains
     * the given value.
     */
    private int findLastBlock(int val) {
        if (count == 0) {
            return -1;
        }

        int low = 0;
        int high = block_list.size() - 1;

        while (low <= high) {

            if (high - low <= 2) {
                for (int i = high; i >= low; --i) {
                    IntegerListBlockInterface block =
                            (IntegerListBlockInterface) block_list.get(i);
                    if (block.bottomInt() <= val) {
                        if (block.topInt() >= val) {
                            return i;
                        } else {
                            return -(i + 1) - 1;
                        }
                    }
                }
                return -(low + 1);
            }

            int mid = (low + high) / 2;
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(mid);

            // Is what we are searching for lower than the bottom value?
            if (block.bottomInt() > val) {
                high = mid - 1;
            }
            // No, then is it greater than the highest value?
            else if (block.topInt() < val) {
                low = mid + 1;
            }
            // Equal, so highest must be someplace between mid and high.
            else {
                low = mid;
                if (low == high) {
                    return low;
                }
            }
        }

        return -(low + 1);  // key not found.
    }


    /**
     * Throws an error if the list is immutable.  This is called before any
     * mutable operations on the list.  If the list is mutable and empty then
     * an empty block is added to the list.
     */
    private void checkImmutable() {
        if (immutable) {
            throw new Error("List is immutable.");
        }
        // HACK: We have a side effect of checking whether the list is immutable.
        //   If the block list doesn't contain any entries we add one here.  This
        //   hack reduces the memory requirements.
        else if (block_list.size() == 0) {
            insertListBlock(0, newListBlock());
        }
    }

    // ---------- Public methods ----------

    /**
     * Sets the list as immutable (we aren't allowed to change the contents).
     */
    public final void setImmutable() {
        immutable = true;
    }

    /**
     * Returns true if this interface is immutable.
     */
    public final boolean isImmutable() {
        return immutable;
    }


    // ---------- Standard get/set/remove operations ----------
    //  NOTE: Some of these are not optimal.

    /**
     * The number of integers that are in the list.
     */
    public final int size() {
        return count;
    }

    /**
     * Returns the int at the given position in the list.  NOTE: This is not
     * a very fast routine.  Certainly a lot slower than IntegerVector intAt.
     */
    public final int get(int pos) {
        int size = block_list.size();
        int start = 0;
        for (Object o : block_list) {
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) o;
            int bsize = block.size();
            if (pos >= start && pos < start + bsize) {
                return block.intAt(pos - start);
            }
            start += bsize;
        }
        throw new Error("'pos' (" + pos + ") out of bounds.");
    }

    /**
     * Adds an int at the given position in the list.
     */
    public final void add(int val, int pos) {
        checkImmutable();

        int size = block_list.size();
        int start = 0;
        for (int i = 0; i < size; ++i) {
            Object ob = block_list.get(i);
            IntegerListBlockInterface block = (IntegerListBlockInterface) ob;
            int bsize = block.size();
            if (pos >= start && pos <= start + bsize) {
                insertIntoBlock(val, i, block, pos - start);
                return;
            }
            start += bsize;
        }
        throw new Error("'pos' (" + pos + ") out of bounds.");
    }

    /**
     * Adds an int to the end of the list.
     */
    public final void add(int val) {
        checkImmutable();

        int size = block_list.size();
        IntegerListBlockInterface block =
                (IntegerListBlockInterface) block_list.get(size - 1);
        insertIntoBlock(val, size - 1, block, block.size());
    }

    /**
     * Removes an int from the given position in the list.  Returns the value
     * that used to be at that position.
     */
    public final int remove(int pos) {
        checkImmutable();

        int size = block_list.size();
        int start = 0;
        for (int i = 0; i < size; ++i) {
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(i);
            int bsize = block.size();
            if (pos >= start && pos <= start + bsize) {
                return removeFromBlock(i, block, pos - start);
            }
            start += bsize;
        }
        throw new Error("'pos' (" + pos + ") out of bounds.");
    }

    // ---------- Fast methods ----------

    /**
     * Assuming the list is sorted, this performs a binary search and returns
     * true if the value is found, otherwise returns false.
     * <p>
     * We must supply a 'IndexComparator' for how the list is sorted.
     */
    public final boolean contains(int val) {
        int block_num = findLastBlock(val);

        if (block_num < 0) {
            // We didn't find in the list, so return false.
            return false;
        }

        // We got a block, so find out if it's in the block or not.
        IntegerListBlockInterface block =
                (IntegerListBlockInterface) block_list.get(block_num);

        // Find, if not there then return false.
        int sr = block.searchLast(val);
        return sr >= 0;

    }

    /**
     * Inserts plain 'int' values into the list in sorted order.
     */
    public final void insertSort(int val) {
        checkImmutable();

        int block_num = findLastBlock(val);

        if (block_num < 0) {
            // Not found a block,
            // The block to insert the value,
            block_num = (-(block_num + 1)) - 1;
            if (block_num < 0) {
                block_num = 0;
            }
        }

        // We got a block, so find out if it's in the block or not.
        IntegerListBlockInterface block =
                (IntegerListBlockInterface) block_list.get(block_num);

        // The point to insert in the block,
        int i = block.searchLast(val);
        if (i < 0) {
            i = -(i + 1);
        } else {
            i = i + 1;
            // NOTE: A block can never become totally full so it's always okay to
            //   skip one ahead.
        }

        // Insert value into the block,
        insertIntoBlock(val, block_num, block, i);

    }

    /**
     * Inserts plain 'int' value into the sorted position in the list only if
     * it isn't already in the list.  If the value is inserted it returns true,
     * otherwise if the value wasn't inserted because it's already in the list,
     * it returns false.
     */
    public final boolean uniqueInsertSort(int val) {
        checkImmutable();

        int block_num = findLastBlock(val);

        if (block_num < 0) {
            // Not found a block,
            // The block to insert the value,
            block_num = (-(block_num + 1)) - 1;
            if (block_num < 0) {
                block_num = 0;
            }
        }

        // We got a block, so find out if it's in the block or not.
        IntegerListBlockInterface block =
                (IntegerListBlockInterface) block_list.get(block_num);

        // The point to insert in the block,
        int i = block.searchLast(val);
        if (i < 0) {
            i = -(i + 1);
        } else {
            // This means we found the value in the given block, so return false.
            return false;
        }

        // Insert value into the block,
        insertIntoBlock(val, block_num, block, i);

        // Value inserted so return true.
        return true;

    }

    /**
     * Removes a plain 'int' value from the sorted position in the list only if
     * it's already in the list.  If the value is removed it returns true,
     * otherwise if the value wasn't removed because it couldn't be found in the
     * list, it returns false.
     */
    public final boolean removeSort(int val) {
        checkImmutable();

        int block_num = findLastBlock(val);

        if (block_num < 0) {
            // Not found a block,
            // The block to remove the value,
            block_num = (-(block_num + 1)) - 1;
            if (block_num < 0) {
                block_num = 0;
            }
        }

        // We got a block, so find out if it's in the block or not.
        IntegerListBlockInterface block =
                (IntegerListBlockInterface) block_list.get(block_num);

        // The point to remove the block,
        int i = block.searchLast(val);
        if (i < 0) {
            // This means we can't found the value in the given block, so return
            // false.
            return false;
        }

        // Remove value into the block,
        int val_removed = removeFromBlock(block_num, block, i);
        if (val != val_removed) {
            throw new Error("Incorrect value removed.");
        }

        // Value removed so return true.
        return true;

    }


    /**
     * Assuming the list is sorted, this performs a binary search and returns
     * true if the value is found, otherwise returns false.
     * <p>
     * We must supply a 'IndexComparator' for how the list is sorted.
     */
    public final boolean contains(Object key, IndexComparator c) {
        int block_num = findBlockContaining(key, c);

        if (block_num < 0) {
            // We didn't find in the list, so return false.
            return false;
        }

        // We got a block, so find out if it's in the block or not.
        IntegerListBlockInterface block =
                (IntegerListBlockInterface) block_list.get(block_num);

        // Find, if not there then return false.
        int sr = block.binarySearch(key, c);
        return sr >= 0;

    }

    /**
     * Inserts the key/index pair into the list at the correct sorted position
     * (determine by the IndexComparator).  If the list already contains
     * identical key then the value is put on the end of the set.  This way,
     * the sort is stable (the order of identical elements does not change).
     */
    public final void insertSort(Object key, int val, IndexComparator c) {
        checkImmutable();

        int block_num = findLastBlock(key, c);

        if (block_num < 0) {
            // Not found a block,
            // The block to insert the value,
            block_num = (-(block_num + 1)) - 1;
            if (block_num < 0) {
                block_num = 0;
            }
        }

        // We got a block, so find out if it's in the block or not.
        IntegerListBlockInterface block =
                (IntegerListBlockInterface) block_list.get(block_num);

        // The point to insert in the block,
        int i = block.searchLast(key, c);
        if (i < 0) {
            i = -(i + 1);
        } else {
            i = i + 1;
            // NOTE: A block can never become totally full so it's always okay to
            //   skip one ahead.
        }

        // Insert value into the block,
        insertIntoBlock(val, block_num, block, i);

    }

    /**
     * Removes the key/val pair from the list by first searching for it, and then
     * removing it from the list.
     * <p>
     * NOTE: There will be a list scan (bad performance) for the erronious case
     *   when the key/val pair is not present in the list.
     */
    public final int removeSort(Object key, int val, IndexComparator c) {
        checkImmutable();

        // Find the range of blocks that the value is in.
        final int orig_block_num = findFirstBlock(key, c);
        int block_num = orig_block_num;
        int l_block_num = block_list.size() - 1;
//    int l_block_num = findLastBlock(key, c);

        if (block_num < 0) {
            // Not found in a block,
            throw new Error("Value (" + key + ") was not found in the list.");
        }

//    int i = -1;
        IntegerListBlockInterface block =
                (IntegerListBlockInterface) block_list.get(block_num);
//    int search_from = block.searchFirst(key, c);
        int i = block.iterativeSearch(val);
        while (i == -1) {
            // If not found, go to next block
            ++block_num;
            if (block_num > l_block_num) {
                throw new Error("Value (" + key + ") was not found in the list.");
            }
            block = (IntegerListBlockInterface) block_list.get(block_num);
            // Try and find the value within this block
            i = block.iterativeSearch(val);
        }

//    int last_block_num = findLastBlock(key, c);
//    if (last_block_num > orig_block_num) {
//      double percent = (double) (block_num - orig_block_num) /
//                       (double) (last_block_num - orig_block_num);
//      System.out.println("Block range: " + orig_block_num + " to " +
//                         last_block_num + " p: " + percent);
//    }

        // Remove value from the block,
        return removeFromBlock(block_num, block, i);

    }


    /**
     * Returns the index of the last value in this set that equals the given
     * value.
     */
    public final int searchLast(Object key, IndexComparator c) {
        int block_num = findLastBlock(key, c);
        int sr;

        if (block_num < 0) {
            // Guarenteed not found in any blocks so return start of insert block
            block_num = (-(block_num + 1)); // - 1;
            sr = -1;
        } else {
            // We got a block, so find out if it's in the block or not.
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(block_num);

            // Try and find it in the block,
            sr = block.searchLast(key, c);
        }

        int offset = 0;
        for (int i = 0; i < block_num; ++i) {
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(i);
            offset += block.size();
        }

        if (sr >= 0) {
            return offset + sr;
        } else {
            return -offset + sr;
        }

    }

    /**
     * Returns the index of the first value in this set that equals the given
     * value.
     */
    public final int searchFirst(Object key, IndexComparator c) {
        int block_num = findFirstBlock(key, c);
        int sr;

        if (block_num < 0) {
            // Guarenteed not found in any blocks so return start of insert block
            block_num = (-(block_num + 1)); // - 1;
//      System.out.println("BN (" + key + "): " + block_num);
            sr = -1;
        } else {
            // We got a block, so find out if it's in the block or not.
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(block_num);

            // Try and find it in the block,
            sr = block.searchFirst(key, c);
        }

        int offset = 0;
        for (int i = 0; i < block_num; ++i) {
            IntegerListBlockInterface block =
                    (IntegerListBlockInterface) block_list.get(i);
            offset += block.size();
        }

        if (sr >= 0) {
            return offset + sr;
        } else {
            return -offset + sr;
        }

    }

    // ---------- Iterator operations ----------


    /**
     * Our iterator that walks through the list.
     */
    private final class BILIterator implements IntegerIterator {


        private final int start_offset;
        private int end_offset;
        private IntegerListBlockInterface current_block;
        private int current_block_size;
        private int block_index;
        private int block_offset;
        private int cur_offset;

        public BILIterator(int start_offset, int end_offset) {
            this.start_offset = start_offset;
            this.end_offset = end_offset;
            cur_offset = start_offset - 1;

            if (end_offset >= start_offset) {
                // Setup variables to 1 before the start
                setupVars(start_offset - 1);
            }

        }

        /**
         * Sets up the internal variables given an offset.
         */
        private void setupVars(int pos) {
            int size = block_list.size();
            int start = 0;
            for (block_index = 0; block_index < size; ++block_index) {
                IntegerListBlockInterface block =
                        (IntegerListBlockInterface) block_list.get(block_index);
                int bsize = block.size();
                if (pos < start + bsize) {
                    block_offset = pos - start;
                    if (block_offset < 0) {
                        block_offset = -1;
                    }
                    current_block = block;
                    current_block_size = bsize;
                    return;
                }
                start += bsize;
            }
            throw new Error("'pos' (" + pos + ") out of bounds.");
        }


        // ---------- Implemented from IntegerIterator ----------

        public boolean hasNext() {
            return cur_offset < end_offset;
        }

        public int next() {
            ++block_offset;
            ++cur_offset;
            if (block_offset >= current_block_size) {
                ++block_index;
                current_block =
                        (IntegerListBlockInterface) block_list.get(block_index);
//        current_block = current_block.next;
                current_block_size = current_block.size();
                block_offset = 0;
            }
            return current_block.intAt(block_offset);
        }

        public boolean hasPrevious() {
            return cur_offset > start_offset;
        }

        private void walkBack() {
            --block_offset;
            --cur_offset;
            if (block_offset < 0) {
                if (block_index > 0) {
//        if (current_block.previous != null) {
                    --block_index;
                    current_block =
                            (IntegerListBlockInterface) block_list.get(block_index);
//          current_block = current_block.previous;
                    current_block_size = current_block.size();
                    block_offset = current_block.size() - 1;
                }
            }
        }

        public int previous() {
            walkBack();
            return current_block.intAt(block_offset);
        }

        public void remove() {
            checkImmutable();

            // NOT ELEGANT: We check 'block_list' size to determine if the value
            //   deletion caused blocks to be removed.  If it did, we set up the
            //   internal variables afresh with a call to 'setupVars'.
            int orig_block_count = block_list.size();
            removeFromBlock(block_index, current_block, block_offset);

            // Did the number of blocks in the list change?
            if (orig_block_count == block_list.size()) {
                // HACK: Reduce the current cached block size
                --current_block_size;
                walkBack();
            } else {
                --cur_offset;
                setupVars(cur_offset);
            }
            --end_offset;
        }

    }


    /**
     * Returns an IntegerIterator that will walk from the start offset
     * (inclusive) to the end offset (inclusive) of this list.
     * <p>
     * This iterator supports the 'remove' method.
     */
    public IntegerIterator iterator(int start_offset, int end_offset) {
        return new BILIterator(start_offset, end_offset);
    }

    /**
     * Returns an IntegerIterator that will walk from the start to the end
     * this list.
     * <p>
     * This iterator supports the 'remove' method.
     */
    public IntegerIterator iterator() {
        return iterator(0, size() - 1);
    }


    // ---------- Debugging ----------

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("Blocks: " + block_list.size() + "\n");
        for (int i = 0; i < block_list.size(); ++i) {
            buf.append("Block (" + i + "): " + block_list.get(i).toString() + "\n");
        }
        return new String(buf);
    }

    public void checkSorted(IndexComparator c) {
        IntegerIterator iterator = iterator(0, size() - 1);
        checkSorted(iterator, c);
    }

    public static void checkSorted(IntegerIterator iterator, IndexComparator c) {
        if (iterator.hasNext()) {
            int last_index = iterator.next();
            while (iterator.hasNext()) {
                int cur = iterator.next();
                if (c.compare(cur, last_index) < 0) {
                    throw new Error("List not sorted!");
                }
                last_index = cur;
            }
        }
    }


}
