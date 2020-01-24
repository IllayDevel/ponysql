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

import java.util.Arrays;

/**
 * Provides various sort utilities for a list of objects that implement
 * Comparable.  It also provide some methods that can be used on a sorted
 * list of objects, such as a fast search method.
 * <p>
 * All the methods in this class are static.
 * <p>
 * @author Tobias Downer
 */

public final class SortUtil {

    /**
     * Performs a quick sort on the given array of Comparable objects between
     * the min and maximum range.  It changes the array to the new sorted order.
     */
    public static void quickSort(Comparable[] list, int min, int max) {
        Arrays.sort(list, min, max + 1);

//    int left = min;
//    int right = max;
//
//    if (max > min) {
//      Comparable mid = list[(min + max) / 2];
//      while (left < right) {
//        while (left < max && list[left].compareTo(mid) < 0) {
//          ++left;
//        }
//        while (right > min && list[right].compareTo(mid) > 0) {
//          --right;
//        }
//        if (left <= right) {
//          if (left != right) {
//            Comparable t = list[left];
//            list[left] = list[right];
//            list[right] = t;
//          }
//
//          ++left;
//          --right;
//        }
//
//      }
//
//      if (min < right) {
//        quickSort(list, min, right);
//      }
//      if (left < max) {
//        quickSort(list, left, max);
//      }
//
//    }
    }

    /**
     * Performs a quick sort on the given array of Comparable objects.
     * It changes the array to the new sorted order.
     */
    public static void quickSort(Comparable[] obs) {
        quickSort(obs, 0, obs.length - 1);
    }


    /**
     * Quickly finds the index of the given object in the list.  If the object
     * can not be found, it returns the point where the element should be
     * added.
     */
    public static int sortedIndexOf(Comparable[] list, Comparable val, int lower, int higher) {

        if (lower >= higher) {
            if (val.compareTo(list[lower]) > 0) {
                return lower + 1;
            } else {
                return lower;
            }
        }

        int mid = (lower + higher) / 2;
        Comparable mid_val = list[mid];

        if (val.equals(mid_val)) {
            return mid;
        } else if (val.compareTo(mid_val) < 0) {
            return sortedIndexOf(list, val, lower, mid - 1);
        } else {
            return sortedIndexOf(list, val, mid + 1, higher);
        }

    }

    /**
     * Quickly finds the given element in the array of objects.  It assumes
     * the object given is of the same type as the array.  It returns null if
     * the given object is not in the array.  If the object is in the array, it
     * returns a SearchResults object that contains information about the index
     * of the found object, and the number of objects like this in the array.
     * Not that it takes a 'SearchResults' object to store the results in.  This
     * is for reuse of an old SearchResults object that is no longer needed.
     * This prevents gc and overhead of having to allocate a new SearchResults
     * object.
     * This method works by dividing the search space in half and iterating in
     * the half with the given object in.
     */
    public static SearchResults sortedQuickFind(Comparable[] list, Comparable val, SearchResults results) {
        if (list.length == 0) {
            return null;
        }

        int size = list.length - 1;
        int count = 0;

        int i = sortedIndexOf(list, val, 0, size);
        if (i > size) {
            return null;
        }
        int temp_i = i;

        while (temp_i >= 0 && list[temp_i].equals(val)) {
            ++count;
            --temp_i;
        }
        int start_index = temp_i + 1;
        temp_i = i + 1;
        while (temp_i <= size && list[temp_i].equals(val)) {
            ++count;
            ++temp_i;
        }

        if (count == 0) {
            return null;
        } else {
            if (results == null) {
                results = new SearchResults();
            }
            results.found_index = start_index;
            results.found_count = count;
        }

        return results;
    }

}
