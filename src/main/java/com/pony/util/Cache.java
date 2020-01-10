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
 * Represents a cache of Objects.  A Cache is similar to a Hashtable, in that
 * you can 'add' and 'get' objects from the container given some key.  However
 * a cache may remove objects from the container when it becomes too full.
 * <p>
 * The cache scheme uses a doubly linked-list hashtable.  The most recently
 * accessed objects are moved to the start of the list.  The end elements in
 * the list are wiped if the cache becomes too full.
 * <p>
 * @author Tobias Downer
 */

public class Cache {

    /**
     * The maximum number of DataCell objects that can be stored in the cache
     * at any one time.
     */
    private int max_cache_size;

    /**
     * The current cache size.
     */
    private int current_cache_size;

    /**
     * The number of nodes that should be left available when the cache becomes
     * too full and a clean up operation occurs.
     */
    private int wipe_to;

    /**
     * The array of ListNode objects arranged by hashing value.
     */
    private final ListNode[] node_hash;

    /**
     * A pointer to the start of the list.
     */
    private ListNode list_start;

    /**
     * A pointer to the end of the list.
     */
    private ListNode list_end;

    /**
     * The Constructors.  It takes a maximum size the cache can grow to, and the
     * percentage of the cache that is wiped when it becomes too full.
     */
    public Cache(int hash_size, int max_size, int clean_percentage) {
        if (clean_percentage >= 85) {
            throw new RuntimeException(
                    "Can't set to wipe more than 85% of the cache during clean.");
        }
        max_cache_size = max_size;
        current_cache_size = 0;
        wipe_to = max_size - ((clean_percentage * max_size) / 100);

        node_hash = new ListNode[hash_size];

        list_start = null;
        list_end = null;
    }

    public Cache(int max_size, int clean_percentage) {
        this((max_size * 2) + 1, max_size, 20);
    }

    public Cache(int max_size) {
        this(max_size, 20);
    }

    public Cache() {
        this(50);
    }

    /**
     * Creates the HashMap object to store objects in this cache.  This is
     * available to be overwritten.
     * @deprecated
     */
    protected final int getHashSize() {
        return (max_cache_size * 2) + 1;
    }


    /**
     * This is called whenever at Object is put into the cache.  This method
     * should determine if the cache should be cleaned and call the clean
     * method if appropriate.
     */
    protected void checkClean() {
        // If we have reached maximum cache size, remove some elements from the
        // end of the list
        if (current_cache_size >= max_cache_size) {
            clean();
        }
    }

    /**
     * Returns true if the clean-up method that periodically cleans up the
     * cache, should clean up more elements from the cache.
     */
    protected boolean shouldWipeMoreNodes() {
        return (current_cache_size >= wipe_to);
    }

    /**
     * Notifies that the given object has been wiped from the cache by the
     * clean up procedure.
     */
    protected void notifyWipingNode(Object ob) {
    }

    /**
     * Notifies that some statistical information about the hash map has
     * updated.  This should be used to compile statistical information about
     * the number of walks a 'get' operation takes to retreive an entry from
     * the hash.
     * <p>
     * This method is called every 8192 gets.
     */
    protected void notifyGetWalks(long total_walks, long total_get_ops) {
    }

    // ---------- Hashing methods ----------

    /**
     * Some statistics about the hashing algorithm.
     */
    private long total_gets = 0;
    private long get_total = 0;

    /**
     * Finds the node with the given key in the hash table and returns it.
     * Returns 'null' if the value isn't in the hash table.
     */
    private ListNode getFromHash(Object key) {
        int hash = key.hashCode();
        int index = (hash & 0x7FFFFFFF) % node_hash.length;
        int get_count = 1;

        for (ListNode e = node_hash[index]; e != null; e = e.next_hash_entry) {
            if (key.equals(e.key)) {
                ++total_gets;
                get_total += get_count;

                // Every 8192 gets, call the 'notifyGetWalks' method with the
                // statistical info.
                if ((total_gets & 0x01FFF) == 0) {
                    try {
                        notifyGetWalks(get_total, total_gets);
                        // Reset stats if we overflow on an int
                        if (get_total > (65536 * 65536)) {
                            get_total = 0;
                            total_gets = 0;
                        }
                    } catch (Throwable except) { /* ignore */ }
                }

                // Bring to head if get_count > 1
                if (get_count > 1) {
                    bringToHead(e);
                }
                return e;
            }
            ++get_count;
        }
        return null;
    }

    /**
     * Puts the node with the given key into the hash table.
     */
    private ListNode putIntoHash(ListNode node) {
        // Makes sure the key is not already in the HashMap.
        int hash = node.key.hashCode();
        int index = (hash & 0x7FFFFFFF) % node_hash.length;
        Object key = node.key;
        for (ListNode e = node_hash[index]; e != null; e = e.next_hash_entry) {
            if (key.equals(e.key)) {
                throw new Error(
                        "ListNode with same key already in the hash - remove first.");
            }
        }

        // Stick it in the hash list.
        node.next_hash_entry = node_hash[index];
        node_hash[index] = node;

        return node;
    }

    /**
     * Removes the given node from the hash table.  Returns 'null' if the entry
     * wasn't found in the hash.
     */
    private ListNode removeFromHash(Object key) {
        // Makes sure the key is not already in the HashMap.
        int hash = key.hashCode();
        int index = (hash & 0x7FFFFFFF) % node_hash.length;
        ListNode prev = null;
        for (ListNode e = node_hash[index]; e != null; e = e.next_hash_entry) {
            if (key.equals(e.key)) {
                // Found entry, so remove it baby!
                if (prev == null) {
                    node_hash[index] = e.next_hash_entry;
                } else {
                    prev.next_hash_entry = e.next_hash_entry;
                }
                return e;
            }
            prev = e;
        }

        // Not found so return 'null'
        return null;
    }

    /**
     * Clears the entire hashtable of all entries.
     */
    private void clearHash() {
        for (int i = node_hash.length - 1; i >= 0; --i) {
            node_hash[i] = null;
        }
    }


    // ---------- Public cache methods ----------

    /**
     * Returns the number of nodes that are currently being stored in the
     * cache.
     */
    public final int nodeCount() {
        return current_cache_size;
    }

    /**
     * Puts an Object into the cache with the given key.
     */
    public final void put(Object key, Object ob) {

        // Do we need to clean any cache elements out?
        checkClean();

        // Check whether the given key is already in the Hashtable.

        ListNode node = getFromHash(key);
        if (node == null) {

            node = createListNode();
            node.key = key;
            node.contents = ob;

            // Add node to top.
            node.next = list_start;
            node.previous = null;
            list_start = node;
            if (node.next == null) {
                list_end = node;
            } else {
                node.next.previous = node;
            }

            ++current_cache_size;

            // Add node to key mapping
            putIntoHash(node);

        } else {

            // If key already in Hashtable, all we need to do is set node with
            // the new contents and bring the node to the start of the list.

            node.contents = ob;
            bringToHead(node);

        }

    }

    /**
     * If the cache contains the cell with the given key, this method will
     * return the object.  If the cell is not in the cache, it returns null.
     */
    public final Object get(Object key) {
        ListNode node = getFromHash(key);

        if (node != null) {
            // Bring node to start of list.
//      bringToHead(node);

            return node.contents;
        }

        return null;
    }

    /**
     * Ensures that there is no cell with the given key in the cache.  This is
     * useful for ensuring the cache does not contain out-dated information.
     */
    public final Object remove(Object key) {
        ListNode node = removeFromHash(key);

        if (node != null) {
            // If removed node at head.
            if (list_start == node) {
                list_start = node.next;
                if (list_start != null) {
                    list_start.previous = null;
                } else {
                    list_end = null;
                }
            }
            // If removed node at end.
            else if (list_end == node) {
                list_end = node.previous;
                if (list_end != null) {
                    list_end.next = null;
                } else {
                    list_start = null;
                }
            } else {
                node.previous.next = node.next;
                node.next.previous = node.previous;
            }

            --current_cache_size;

            Object contents = node.contents;

            // Set internals to null to ensure objects get gc'd
            node.contents = null;
            node.key = null;

            return contents;
        }

        return null;
    }

    /**
     * Clear the cache of all the entries.
     */
    public void removeAll() {
        if (current_cache_size != 0) {
            current_cache_size = 0;
            clearHash();
        }
        list_start = null;
        list_end = null;
    }

    public void clear() {
        removeAll();
    }


    /**
     * Creates a new ListNode.  If there is a free ListNode on the
     * 'recycled_nodes' then it obtains one from there, else it creates a new
     * blank one.
     */
    private ListNode createListNode() {
        return new ListNode();
    }

    /**
     * Cleans away some old elements in the cache.  This method walks from the
     * end, back 'wipe_count' elements putting each object on the recycle stack.
     *
     */
    protected final int clean() {

        ListNode node = list_end;
        if (node == null) {
            return 0;
        }

        int actual_count = 0;
        while (node != null && shouldWipeMoreNodes()) {
            notifyWipingNode(node.contents);

            removeFromHash(node.key);
            // Help garbage collector with old objects
            node.contents = null;
            node.key = null;
            ListNode old_node = node;
            // Move to previous node
            node = node.previous;

            // Help the GC by clearing away the linked list nodes
            old_node.next = null;
            old_node.previous = null;

            --current_cache_size;
            ++actual_count;
        }

        if (node != null) {
            node.next = null;
            list_end = node;
        } else {
            list_start = null;
            list_end = null;
        }

        return actual_count;
    }

    /**
     * Brings 'node' to the start of the list.  Only nodes at the end of the
     * list are cleaned.
     */
    private void bringToHead(ListNode node) {
        if (list_start != node) {

            ListNode next_node = node.next;
            ListNode previous_node = node.previous;

            node.next = list_start;
            node.previous = null;
            list_start = node;
            node.next.previous = node;

            if (next_node != null) {
                next_node.previous = previous_node;
            } else {
                list_end = previous_node;
            }
            previous_node.next = next_node;

        }
    }


    // ---------- Inner classes ----------

    /**
     * An element in the linked list structure.
     */
    static final class ListNode {

        /**
         * Links to the next and previous nodes.  The ends of the list are 'null'
         */
        ListNode next;
        ListNode previous;

        /**
         * The next node in the hash link on this hash value, or 'null' if last
         * hash entry.
         */
        ListNode next_hash_entry;


        /**
         * The key in the Hashtable for this object.
         */
        Object key;

        /**
         * The object contents for this element.
         */
        Object contents;

    }

}
