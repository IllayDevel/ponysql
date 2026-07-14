/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

package com.pony.util;

import java.util.Vector;

/**
 * A utility container class for holding a list of strings.  This method
 * provides a convenient way of exporting and importing the list as a string
 * itself.  This is useful if we need to represent a variable array of
 * strings.
 *
 * @author Tobias Downer
 */

public class StringListBucket {

    /**
     * The String List.
     */
    private final Vector string_list;

    /**
     * Constructs the bucket.
     */
    public StringListBucket() {
        string_list = new Vector();
    }

    public StringListBucket(String list) {
        this();
        fromString(list);
    }


    /**
     * Returns the number of string elements in the list.
     */
    public int size() {
        return string_list.size();
    }

    /**
     * Clears the list of all string elements.
     */
    public void clear() {
        string_list.clear();
    }

    /**
     * Adds a string to the end of the list.
     */
    public void add(String element) {
        string_list.addElement(element);
    }

    /**
     * Adds a string to the given index of the list.
     */
    public void add(String element, int index) {
        string_list.insertElementAt(element, index);
    }

    /**
     * Returns the string at the given index of the list.
     */
    public String get(int index) {
        return (String) string_list.elementAt(index);
    }

    /**
     * Removes the string at the given index of the list.
     */
    public void remove(int index) {
        string_list.removeElementAt(index);
    }

    /**
     * Returns true if the list contains the given element string.
     */
    public boolean contains(String element) {
        return string_list.contains(element);
    }

    /**
     * Returns the index of the given string in the bucket, or -1 if not found.
     */
    public int indexOfVar(String element) {
        return string_list.indexOf(element);
    }

    /**
     * Returns the bucket as a StringBuffer.  This can be exported to a file
     * or to a database, etc.
     */
    public StringBuffer toStringBuffer() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("||");
        for (int i = 0; i < size(); ++i) {
            String str = get(i);
            buffer.append(str);
            buffer.append("||");
        }
        return buffer;
    }

    public String toString() {
        return toStringBuffer().toString();
    }

    /**
     * Imports from a String into this bucket.  This is used to transform a
     * previously exported bucket via 'toStringBuffer()'.
     */
    public void fromString(String list) {
        clear();
        if (list != null && list.length() > 2) {
            int last = 2;
            int i = list.indexOf("||", 2);
            while (i != -1) {
                String entry = list.substring(last, i);
                add(entry);
                last = i + 2;
                i = list.indexOf("||", last);
            }
        }
    }

}
