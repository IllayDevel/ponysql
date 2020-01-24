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

package com.pony.util;

/**
 * Similar to the Vector class, except this can only store integer values.
 * <p>
 * @author Tobias Downer
 */

public final class IntegerVector implements java.io.Serializable {

    /**
     * The int array.
     */
    protected int[] list;

    /**
     * The index of the last value of the array.
     */
    protected int index;

    /**
     * The Constructors.
     */
    public IntegerVector() {
        this(32);
    }

    public IntegerVector(int initial_list_size) {
        index = 0;
        list = new int[initial_list_size];
    }

    public IntegerVector(IntegerVector vec) {
        if (vec != null && vec.list != null) {
            list = new int[vec.list.length];
            index = vec.index;
            System.arraycopy(vec.list, 0, list, 0, index);
        } else {
            index = 0;
            list = new int[0];
        }
    }

    public IntegerVector(IntegerListInterface i_list) {
        this(i_list.size());
        if (i_list instanceof AbstractBlockIntegerList) {
            AbstractBlockIntegerList bilist = (AbstractBlockIntegerList) i_list;
            int bill_size = bilist.size();
            bilist.copyToArray(list, 0, bill_size);
            index = bill_size;
        } else {
            IntegerIterator i = i_list.iterator();
            // NOTE: We are guarenteed the size of the 'list' array matches the size
            //   of input list.
            while (i.hasNext()) {
                list[index] = i.next();
                ++index;
            }
        }
    }


    /**
     * Ensures there's enough room to make a single addition to the list.
     */
    private void ensureCapacityForAddition() {
        if (index >= list.length) {
            int[] old_arr = list;

            int grow_size = old_arr.length + 1;
            // Put a cap on the new size.
            if (grow_size > 35000) {
                grow_size = 35000;
            }

            int new_size = old_arr.length + grow_size;
            list = new int[new_size];
            System.arraycopy(old_arr, 0, list, 0, index);
        }
    }

    /**
     * Ensures there's enough room to make 'n' additions to the list.
     */
    private void ensureCapacityForAdditions(int n) {
        int intended_size = index + n;
        if (intended_size > list.length) {
            int[] old_arr = list;

            int grow_size = old_arr.length + 1;
            // Put a cap on the new size.
            if (grow_size > 35000) {
                grow_size = 35000;
            }

            int new_size = Math.max(old_arr.length + grow_size, intended_size);
            list = new int[new_size];
            System.arraycopy(old_arr, 0, list, 0, index);
        }
    }

    /**
     * Adds an int to the vector.
     */
    public void addInt(int val) {
//    if (list == null) {
//      list = new int[64];
//    }

        ensureCapacityForAddition();

        list[index] = val;
        ++index;
    }

    /**
     * Removes an Int from the specified position in the list.
     */
    public void removeIntAt(int pos) {
        --index;
        System.arraycopy(list, pos + 1, list, pos, (index - pos));
    }

    /**
     * Removes the first Int found that matched the specified value.
     */
    public void removeInt(int val) {
        int pos = indexOf(val);
        if (pos == -1) {
            throw new RuntimeException("Tried to remove none existant int.");
        }
        removeIntAt(pos);
    }

    /**
     * Crops the IntegerVector so it only contains values between start
     * (inclusive) and end (exclusive).  So;
     *   crop({ 4, 5, 4, 3, 9, 7 }, 0, 3)
     *   would return {4, 5, 4)
     * and,
     *   crop({ 4, 5, 4, 3, 9, 7 }, 3, 4)
     *   would return {3}
     */
    public void crop(int start, int end) {
        if (start < 0) {
            throw new Error("Crop start < 0.");
        } else if (start == 0) {
            if (end > index) {
                throw new Error("Crop end was past end.");
            }
            index = end;
        } else {
            if (start >= index) {
                throw new Error("start >= index");
            }
            int length = (end - start);
            if (length < 0) {
                throw new Error("end - start < 0");
            }
            System.arraycopy(list, start, list, 0, length);
            index = length;
        }
    }


    /**
     * Inserts an int at the given position.
     */
    public void insertIntAt(int val, int pos) {
        if (pos >= index) {
            throw new ArrayIndexOutOfBoundsException(pos + " >= " + index);
        }

//    if (list == null) {
//      list = new int[64];
//    }

        ensureCapacityForAddition();
        System.arraycopy(list, pos, list, pos + 1, (index - pos));
        ++index;
        list[pos] = val;
    }

    /**
     * Sets an int at the given position, overwriting anything that was
     * previously there.  It returns the value that was previously at the element.
     */
    public int setIntAt(int val, int pos) {
        if (pos >= index) {
            throw new ArrayIndexOutOfBoundsException(pos + " >= " + index);
        }

        int old = list[pos];
        list[pos] = val;
        return old;
    }

    /**
     * Places an int at the given position, overwriting anything that was
     * previously there.  It returns the value that was previously at the
     * element.  If 'pos' points to a place outside the bounds of the list then
     * the list is expanded to include this value.
     */
    public int placeIntAt(int val, int pos) {
        int llength = list.length;
        if (pos >= list.length) {
            ensureCapacityForAdditions((llength - index) + (pos - llength) + 5);
        }

        if (pos >= index) {
            index = pos + 1;
        }

        int old = list[pos];
        list[pos] = val;
        return old;
    }


    /**
     * Appends an IntegerVector to the end of the array.  Returns this object.
     */
    public IntegerVector append(IntegerVector vec) {
        if (vec != null) {
            int size = vec.size();
            // Make sure there's enough room for the new array
            ensureCapacityForAdditions(size);

            // Copy the list into this vector.
            System.arraycopy(vec.list, 0, list, index, size);
            index += size;

//      int size = vec.size();
//      for (int i = 0; i < size; ++i) {
//        addInt(vec.intAt(i));
//      }
        }
        return this;
    }

    /**
     * Returns the Int at the given position.
     */
    public int intAt(int pos) {
        if (pos >= index) {
            throw new ArrayIndexOutOfBoundsException(pos + " >= " + index);
        }

        return list[pos];
    }

    /**
     * Returns the first index of the given row in the array, or -1 if not
     * found.
     */
    public int indexOf(int val) {
        for (int i = 0; i < index; ++i) {
            if (list[i] == val) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns true if the vector contains the given value.
     */
    public boolean contains(int val) {
        return (indexOf(val) != -1);
    }

    /**
     * Returns the size of the vector.
     */
    public int getSize() {
        return index;
    }

    /**
     * Returns the size of the vector.
     */
    public int size() {
        return index;
    }

    /**
     * Converts the vector into an int[] array.
     */
    public int[] toIntArray() {
        if (getSize() != 0) {
            int[] out_list = new int[getSize()];
            System.arraycopy(list, 0, out_list, 0, getSize());
            return out_list;
        }
        return null;
    }

    /**
     * Clears the object to be re-used.
     */
    public void clear() {
        index = 0;
    }

    /**
     * Converts the vector into a String.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < index; ++i) {
            buf.append(list[i]);
            buf.append(", ");
        }
        return new String(buf);
    }

    /**
     * Returns true if this vector is equal to the given vector.
     */
    public boolean equals(IntegerVector ivec) {
        int dest_index = ivec.index;
        if (index != dest_index) {
            return false;
        }
        for (int i = 0; i < index; ++i) {
            if (list[i] != ivec.list[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reverses all the list of integers.  So integer[0] is swapped with
     * integer[n - 1], integer[1] is swapped with integer[n - 2], etc where
     * n is the size of the vector.
     */
    public void reverse() {
        final int upper = index - 1;
        final int bounds = index / 2;
        int end_index, temp;

        // Swap ends and interate the two end pointers inwards.
        // i         = lower end
        // upper - i = upper end

        for (int i = 0; i < bounds; ++i) {
            end_index = upper - i;

            temp = list[i];
            list[i] = list[end_index];
            list[end_index] = temp;
        }
    }


    /**
     * Performs a quick sort on the array between the min and max bounds.
     */
    public final void quickSort(int min, int max) {
        int left = min;
        int right = max;

        if (max > min) {
            int mid = list[(min + max) / 2];
            while (left < right) {
                while (left < max && list[left] < mid) {
                    ++left;
                }
                while (right > min && list[right] > mid) {
                    --right;
                }
                if (left <= right) {
                    if (left != right) {
                        int t = list[left];
                        list[left] = list[right];
                        list[right] = t;
                    }

                    ++left;
                    --right;
                }

            }

            if (min < right) {
                quickSort(min, right);
            }
            if (left < max) {
                quickSort(left, max);
            }

        }
    }

    /**
     * Performs a quick sort on the entire vector.
     */
    public final void quickSort() {
        quickSort(0, index - 1);
    }

    /**
     * This is a very quick search for a value given a sorted array.  The search
     * is performed between the lower and higher bounds of the array.  If the
     * requested value is not found, it returns the index where the value should
     * be 'inserted' to maintain a sorted list.
     */
    public final int sortedIndexOf(int val, int lower, int higher) {

        if (lower >= higher) {
            if (lower < index && val > list[lower]) {
                return lower + 1;
            } else {
                return lower;
            }
        }

        int mid = (lower + higher) / 2;
        int mid_val = list[mid];

        if (val == mid_val) {
            return mid;
        } else if (val < mid_val) {
            return sortedIndexOf(val, lower, mid - 1);
        } else {
            return sortedIndexOf(val, mid + 1, higher);
        }

    }

    /**
     * Searches the entire sorted list for the given value and returns the index
     * of it.  If the value is not found, it returns the place in the list where
     * the value should be insorted to maintain a sorted list.
     */
    public final int sortedIndexOf(int val) {
        return sortedIndexOf(val, 0, index - 1);
    }

    /**
     * Given a sorted list, this will return the count of this value in the
     * list.  This uses a quick search algorithm so should be quite fast.
     */
    public final int sortedIntCount(int val) {
        if (index == 0) {
            return 0;
        }

        int count = 0;
        int size = index - 1;

        int i = sortedIndexOf(val, 0, size);
        if (i > size) {
            return 0;
        }
        int temp_i = i;

        while (temp_i >= 0 && list[temp_i] == val) {
            ++count;
            --temp_i;
        }
        temp_i = i + 1;
        while (temp_i <= size && list[temp_i] == val) {
            ++count;
            ++temp_i;
        }

        return count;

    }


    /**
     * Test routine to check vector is sorted.
     */
    public boolean isSorted() {
        int cur = Integer.MIN_VALUE; //-1000000;
        for (int i = 0; i < index; ++i) {
            int a = list[i];
            if (a >= cur) {
                cur = a;
            } else {
                return false;
            }
        }
        return true;
    }

}
