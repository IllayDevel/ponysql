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
import java.util.List;
import java.util.Iterator;
import java.io.*;

/**
 * Various String utilities.
 *
 * @author Tobias Downer
 */

public class StringUtil {

    /**
     * Finds the index of the given string in the source string.
     * <p>
     * @return -1 if the 'find' string could not be found.
     */
    public static int find(String source, String find) {
        return source.indexOf(find);

//    int find_index = 0;
//    int len = source.length();
//    int find_len = find.length();
//    int i = 0;
//    for (; i < len; ++i) {
//      if (find_index == find_len) {
//        return i - find_len;
//      }
//      if (find.indexOf(source.charAt(i), find_index) == find_index) {
//        ++find_index;
//      }
//      else {
//        find_index = 0;
//      }
//    }
//    if (find_index == find_len) {
//      return i - find_len;
//    }
//    else {
//      return -1;
//    }
    }

    /**
     * Performs an 'explode' operation on the given source string.  This
     * algorithm finds all instances of the deliminator string, and returns an
     * array of sub-strings of between the deliminator.  For example,
     * <code>
     *   explode("10:30:40:55", ":") = ({"10", "30", "40", "55"})
     * </code>
     */
    public static List explode(String source, String deliminator) {
        ArrayList list = new ArrayList();
        int i = find(source, deliminator);
        while (i != -1) {
            list.add(source.substring(0, i));
            source = source.substring(i + deliminator.length());
            i = find(source, deliminator);
        }
        list.add(source);
        return list;
    }

    /**
     * This is the inverse of 'explode'.  It forms a string by concatinating
     * each string in the list and seperating each with a deliminator string.
     * For example,
     * <code>
     *   implode(({"1", "150", "500"}), ",") = "1,150,500"
     * </code>
     */
    public static String implode(List list, String deliminator) {
        StringBuffer str = new StringBuffer();
        Iterator iter = list.iterator();
        boolean has_next = iter.hasNext();
        while (has_next) {
            str.append(iter.next().toString());
            has_next = iter.hasNext();
            if (has_next) {
                str.append(deliminator);
            }
        }
        return new String(str);
    }

    /**
     * Searches for various instances of the 'search' string and replaces them
     * with the 'replace' string.
     */
    public static String searchAndReplace(
            String source, String search, String replace) {
        return implode(explode(source, search), replace);
    }

}
