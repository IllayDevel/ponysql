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
 * An implementation of AbstractBlockIntegerList that stores all int values in
 * blocks that are entirely stored in main memory.  This type of structure
 * is useful for large in-memory lists in which add/remove performance must
 * be fast.
 *
 * @author Tobias Downer
 */

public class BlockIntegerList extends AbstractBlockIntegerList {

    /**
     * Constructs the list.
     */
    public BlockIntegerList() {
        super();
    }

    public BlockIntegerList(IntegerVector ivec) {
        super(ivec);
    }

    /**
     * Copies the information from the given BlockIntegerList into a new
     * object and performs a deep clone of the information in this container.
     */
    public BlockIntegerList(IntegerListInterface i_list) {
        super(i_list);
    }

    // ---------- Block operations ----------

    /**
     * Creates a new ListBlock to fill with ints.
     */
    protected IntegerListBlockInterface newListBlock() {
        return new IntArrayListBlock(512);     // (default block size is 512)
    }

    /**
     * Called when the class decides this ListBlock is no longer needed.
     * Provided as an event for derived classes to intercept.
     */
    protected void deleteListBlock(IntegerListBlockInterface list_block) {
    }

    // ---------- Inner classes ----------

    /**
     * The block that contains the actual int values of the list.  This is
     * made public because it may be useful to derive from this class.
     */
    public static class IntArrayListBlock extends IntegerListBlockInterface {

        /**
         * The array of int's stored in this block.
         */
        protected int[] array;

        /**
         * The number of block entries in this list.
         */
        protected int count;

        /**
         * Blank protected constructor.
         */
        protected IntArrayListBlock() {
            super();
        }

        /**
         * Constructs the block to a specific size.
         */
        public IntArrayListBlock(int block_size) {
            this();
            array = new int[block_size];
            count = 0;
        }

        /**
         * Returns the int[] array for this block.  If 'immutable' is true then
         * the array object is guarenteed to not be mutated.
         */
        protected int[] getArray(boolean immutable) {
            return array;
        }

        /**
         * Returns the count of int's in this block.
         */
        protected int getArrayLength() {
            return array.length;
        }

//    /**
//     * Called just before the array is modified in a block mutation operation.
//     * This is intended to be overwritten to perform advanced array reuse
//     * schemes for cached objects.
//     * <p>
//     * If the block is immutable this method should make it mutable.
//     */
//    protected int[] prepareMutate() {
//      return array;
//    }

        /**
         * Returns the number of entries in this block.
         */
        public final int size() {
            return count;
        }

        /**
         * Returns true if the block is full.
         */
        public final boolean isFull() {
            return count >= getArrayLength();
        }

        /**
         * Returns true if the block is empty.
         */
        public final boolean isEmpty() {
            return count <= 0;
        }

        /**
         * Returns true if the block has enough room to fill with the given number
         * of integers.
         */
        public final boolean canContain(int number) {
            return count + number + 1 < getArrayLength();
        }

        /**
         * The top int in the list.
         */
        public int topInt() {
            return getArray(true)[count - 1];
        }

        /**
         * The bottom int in the list.
         */
        public int bottomInt() {
            if (count > 0) {
                return getArray(true)[0];
            }
            throw new Error("no bottom integer.");
        }

        /**
         * Returns the int at the given position in the array.
         */
        public final int intAt(int pos) {
            return getArray(true)[pos];
        }

        /**
         * Adds an int to the block.
         */
        public final void addInt(int val) {
            has_changed = true;
            int[] arr = getArray(false);
            arr[count] = val;
            ++count;
        }

        /**
         * Removes an Int from the specified position in the block.
         */
        public final int removeIntAt(int pos) {
            has_changed = true;
            int[] arr = getArray(false);
            int val = arr[pos];
//      System.out.println("[" + (pos + 1) + ", " + pos + ", " + (count - pos) + "]");
            System.arraycopy(array, pos + 1, arr, pos, (count - pos));
            --count;
            return val;
        }

        /**
         * Inserts an int at the given position.
         */
        public final void insertIntAt(int val, int pos) {
            has_changed = true;
            int[] arr = getArray(false);
            System.arraycopy(array, pos, arr, pos + 1, (count - pos));
            ++count;
            arr[pos] = val;
        }

        /**
         * Sets an int at the given position, overwriting anything that was
         * previously there.  It returns the value that was previously at the
         * element.
         */
        public final int setIntAt(int val, int pos) {
            has_changed = true;
            int[] arr = getArray(false);
            int old = arr[pos];
            arr[pos] = val;
            return old;
        }

        /**
         * Moves a set of values from the end of this block and inserts it into the
         * given block at the destination index specified.  Assumes the
         * destination block has enough room to store the set.
         */
        public final void moveTo(IntegerListBlockInterface dest_block,
                                 int dest_index, int length) {
            IntArrayListBlock block = (IntArrayListBlock) dest_block;

            int[] arr = getArray(false);
            int[] dest_arr = block.getArray(false);

            // Make room in the destination block
            int destb_size = block.size();
            if (destb_size > 0) {
                System.arraycopy(dest_arr, 0,
                        dest_arr, length, destb_size);
            }
            // Copy from this block into the destination block.
            System.arraycopy(arr, count - length, dest_arr, 0, length);
            // Alter size of destination and source block.
            block.count += length;
            count -= length;
            // Mark both blocks as changed
            has_changed = true;
            block.has_changed = true;
        }

        /**
         * Copies all the data from this block into the given destination block.
         */
        public final void copyTo(IntegerListBlockInterface dest_block) {
            IntArrayListBlock block = (IntArrayListBlock) dest_block;
            int[] dest_arr = block.getArray(false);
            System.arraycopy(getArray(true), 0, dest_arr, 0, count);
            block.count = count;
            block.has_changed = true;
        }

        /**
         * Copies all the data from this block into the given int[] array.  Returns
         * the number of 'int' values copied.
         */
        public final int copyTo(int[] to, int offset) {
            System.arraycopy(getArray(true), 0, to, offset, count);
            return count;
        }

        /**
         * Clears the object to be re-used.
         */
        public final void clear() {
            has_changed = true;
            count = 0;
        }

        /**
         * Performs an iterative search through the int values in the list.
         * If it's found the index of the value is returned, else it returns
         * -1.
         */
        public int iterativeSearch(int val) {
            int[] arr = getArray(true);
            for (int i = count - 1; i >= 0; --i) {
                if (arr[i] == val) {
                    return i;
                }
            }
            return -1;
        }

        /**
         * Performs an iterative search from the given position to the end of
         * the list in the block.
         * If it's found the index of the value is returned, else it returns
         * -1.
         */
        public int iterativeSearch(int val, int position) {
            int[] arr = getArray(true);
            for (int i = position; i < count; ++i) {
                if (arr[i] == val) {
                    return i;
                }
            }
            return -1;
        }


        // ---------- Sort algorithms ----------

        /**
         * Considers each int a reference to another structure, and the block
         * sorted by these structures.  The method performs a binary search.
         */
        public final int binarySearch(Object key, IndexComparator c) {
            int[] arr = getArray(true);
            int low = 0;
            int high = count - 1;

            while (low <= high) {
                int mid = (low + high) / 2;
                int cmp = c.compare(arr[mid], key);

                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return mid; // key found
            }
            return -(low + 1);  // key not found.
        }


        /**
         * Considers each int a reference to another structure, and the block
         * sorted by these structures.  Finds the first index in the block that
         * equals the given key.
         */
        public final int searchFirst(Object key, IndexComparator c) {
            int[] arr = getArray(true);
            int low = 0;
            int high = count - 1;

            while (low <= high) {

                if (high - low <= 2) {
                    for (int i = low; i <= high; ++i) {
                        int cmp = c.compare(arr[i], key);
                        if (cmp == 0) {
                            return i;
                        } else if (cmp > 0) {
                            return -(i + 1);
                        }
                    }
                    return -(high + 2);
                }

                int mid = (low + high) / 2;
                int cmp = c.compare(arr[mid], key);

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    high = mid;
                }
            }
            return -(low + 1);  // key not found.
        }

        /**
         * Considers each int a reference to another structure, and the block
         * sorted by these structures.  Finds the first index in the block that
         * equals the given key.
         */
        public final int searchLast(Object key, IndexComparator c) {
            int[] arr = getArray(true);
            int low = 0;
            int high = count - 1;

            while (low <= high) {

                if (high - low <= 2) {
                    for (int i = high; i >= low; --i) {
                        int cmp = c.compare(arr[i], key);
                        if (cmp == 0) {
                            return i;
                        } else if (cmp < 0) {
                            return -(i + 2);
                        }
                    }
                    return -(low + 1);
                }

                int mid = (low + high) / 2;
                int cmp = c.compare(arr[mid], key);

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    low = mid;
                }
            }
            return -(low + 1);  // key not found.
        }

        /**
         * Assuming a sorted block, finds the first index in the block that
         * equals the given value.
         */
        public final int searchFirst(int val) {
            int[] arr = getArray(true);
            int low = 0;
            int high = count - 1;

            while (low <= high) {

                if (high - low <= 2) {
                    for (int i = low; i <= high; ++i) {
                        if (arr[i] == val) {
                            return i;
                        } else if (arr[i] > val) {
                            return -(i + 1);
                        }
                    }
                    return -(high + 2);
                }

                int mid = (low + high) / 2;

                if (arr[mid] < val) {
                    low = mid + 1;
                } else if (arr[mid] > val) {
                    high = mid - 1;
                } else {
                    high = mid;
                }
            }
            return -(low + 1);  // key not found.
        }

        /**
         * Assuming a sorted block, finds the first index in the block that
         * equals the given value.
         */
        public final int searchLast(int val) {
            int[] arr = getArray(true);
            int low = 0;
            int high = count - 1;

            while (low <= high) {

                if (high - low <= 2) {
                    for (int i = high; i >= low; --i) {
                        if (arr[i] == val) {
                            return i;
                        } else if (arr[i] < val) {
                            return -(i + 2);
                        }
                    }
                    return -(low + 1);
                }

                int mid = (low + high) / 2;

                if (arr[mid] < val) {
                    low = mid + 1;
                } else if (arr[mid] > val) {
                    high = mid - 1;
                } else {
                    low = mid;
                }
            }
            return -(low + 1);  // key not found.
        }


        /**
         * Converts the block into a String.
         */
        public String toString() {
            int[] arr = getArray(true);
            StringBuffer buf = new StringBuffer();
            buf.append("( VALUES: ").append(count).append(" ) ");
            for (int i = 0; i < count; ++i) {
                buf.append(arr[i]);
                buf.append(", ");
            }
            return new String(buf);
        }

    }

}
