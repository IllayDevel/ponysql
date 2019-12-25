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

package com.pony.database;

import com.pony.store.*;
import com.pony.util.ByteArrayUtil;

import java.io.*;
import java.util.ArrayList;

import com.pony.debug.DebugLogger;

/**
 * A store that manages the current state of all tables in a Conglomerate.  It
 * persistantly manages three pieces of information about a conglomerate - the
 * tables that are visible, the tables that are deleted, and a table_id value
 * assigned to new tables that are created.
 *
 * @author Tobias Downer
 */

class StateStore {

    /**
     * The MAGIC value used for state header areas.
     */
    private final int MAGIC = 0x0BAC8001;

    /**
     * The Store object this state store wraps around.
     */
    private final Store store;

    /**
     * The current table identifier.
     */
    private int table_id;

    /**
     * The header area of the state store.  The format of the header area is;
     *   MAGIC(4) - RESERVED(4) - TABLE_ID(8) -
     *   VISIBLE_TABLES_POINTER(8) - DELETED_TABLES_POINTER(8)
     */
    private MutableArea header_area;

    /**
     * Pointer to the visible table area in the store.
     */
    private long vis_p;

    /**
     * Pointer to the delete table area in the store.
     */
    private long del_p;

    /**
     * The list of visible state resources.
     */
    private ArrayList visible_list;

    /**
     * The list of deleted state resources.
     */
    private ArrayList delete_list;

    /**
     * Set to true if the visible list was changed.
     */
    private boolean vis_list_change;

    /**
     * Set to true if the delete list was changed.
     */
    private boolean del_list_change;

    /**
     * Constructs the StateStore.
     */
    public StateStore(Store store) {
        this.store = store;
        vis_list_change = false;
        del_list_change = false;
    }


    /**
     * Removes the given resource from the list.
     */
    private void removeResource(ArrayList list, String name) {
        int sz = list.size();
        for (int i = 0; i < sz; ++i) {
            StateResource resource = (StateResource) list.get(i);
            if (name.equals(resource.name)) {
                list.remove(i);
                return;
            }
        }
        throw new RuntimeException("Couldn't find resource '" + name + "' in list.");
    }

    /**
     * Reads the state resource list from the given area in the store.
     */
    private void readStateResourceList(ArrayList list, long pointer)
            throws IOException {
        DataInputStream d_in = new DataInputStream(
                store.getAreaInputStream(pointer));
        int version = d_in.readInt();   // version
        int count = (int) d_in.readLong();
        for (int i = 0; i < count; ++i) {
            long table_id = d_in.readLong();
            String name = d_in.readUTF();
            StateResource resource = new StateResource(table_id, name);
            list.add(resource);
        }
        d_in.close();
    }

    /**
     * Writes the state resource list to the given area in the store.
     */
    private void writeStateResourceList(ArrayList list, DataOutputStream d_out)
            throws IOException {
        d_out.writeInt(1);
        int sz = list.size();
        d_out.writeLong(sz);
        for (Object o : list) {
            StateResource resource = (StateResource) o;
            d_out.writeLong(resource.table_id);
            d_out.writeUTF(resource.name);
        }
    }

    /**
     * Writes the given list to the store and returns a pointer to the area once
     * the write has finished.
     */
    private long writeListToStore(ArrayList list) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream d_out = new DataOutputStream(bout);
        writeStateResourceList(list, d_out);
        d_out.flush();
        d_out.close();

        byte[] buf = bout.toByteArray();

        AreaWriter a = store.createArea(buf.length);
        long list_p = a.getID();
        a.put(buf);
        a.finish();

        return list_p;
    }

    /**
     * Creates the state store in the store and returns a pointer to the header
     * used later for initializing the state.
     */
    public synchronized long create() throws IOException {
        // Allocate empty visible and deleted tables area
        AreaWriter vis_tables_area = store.createArea(12);
        AreaWriter del_tables_area = store.createArea(12);
        vis_p = vis_tables_area.getID();
        del_p = del_tables_area.getID();

        // Write empty entries for both of these
        vis_tables_area.putInt(1);
        vis_tables_area.putLong(0);
        vis_tables_area.finish();
        del_tables_area.putInt(1);
        del_tables_area.putLong(0);
        del_tables_area.finish();

        // Now allocate an empty state header
        AreaWriter header_writer = store.createArea(32);
        long header_p = header_writer.getID();
        header_writer.putInt(MAGIC);
        header_writer.putInt(0);
        header_writer.putLong(0);
        header_writer.putLong(vis_p);
        header_writer.putLong(del_p);
        header_writer.finish();

        header_area = store.getMutableArea(header_p);

        // Reset table_id
        table_id = 0;

        visible_list = new ArrayList();
        delete_list = new ArrayList();

        // Return pointer to the header area
        return header_p;
    }

    /**
     * Initializes the state store given a pointer to the header area in the
     * store.
     */
    public synchronized void init(long header_p) throws IOException {
        header_area = store.getMutableArea(header_p);
        int mag_value = header_area.getInt();
        if (mag_value != MAGIC) {
            throw new IOException("Magic value for state header area is incorrect.");
        }
        if (header_area.getInt() != 0) {
            throw new IOException("Unknown version for state header area.");
        }
        table_id = (int) header_area.getLong();
        vis_p = header_area.getLong();
        del_p = header_area.getLong();

        // Setup the visible and delete list
        visible_list = new ArrayList();
        delete_list = new ArrayList();

        // Read the resource list for the visible and delete list.
        readStateResourceList(visible_list, vis_p);
        readStateResourceList(delete_list, del_p);

    }

    /**
     * Reads a legacy state file (pre version 1) and converts it to a state store
     * format compatible with this store.  Fortunately the conversion is fairly
     * straight-forward.  This is otherwise the same as using the 'create' method.
     */
    public synchronized long convert(File legacy_sf, DebugLogger debug)
            throws IOException {
        // Create a blank area in the store.
        long header_p = create();

        // Open the state file.
        FixedSizeDataStore state_file =
                new FixedSizeDataStore(legacy_sf, 507, debug);
        state_file.open(true);

        // Read the header.
        byte[] reserved_buffer = new byte[64];
        state_file.readReservedBuffer(reserved_buffer, 0, 64);

        // Read the list of visible tables....
        int tables_sector = ByteArrayUtil.getInt(reserved_buffer, 4);
        InputStream sin = state_file.getSectorInputStream(tables_sector);
        DataInputStream din = new DataInputStream(sin);
        int vtver = din.readInt();   // The version.
        int size = din.readInt();
        // For each committed table,
        for (int i = 0; i < size; ++i) {
            int table_id = din.readInt();
            String resource_name = din.readUTF();
            // Convert to new resource type
            if (!resource_name.startsWith(":")) {
                resource_name = ":1" + resource_name;
            }
            // Add this entry to the visible resource.
            addVisibleResource(new StateResource(table_id, resource_name));
        }
        din.close();

        // Read the list of dropped tables....
        int dropped_sector = ByteArrayUtil.getInt(reserved_buffer, 12);
        if (dropped_sector > -1) {
            sin = state_file.getSectorInputStream(dropped_sector);
            din = new DataInputStream(sin);
            int dsver = din.readInt();  // The version.
            size = din.readInt();
            // For each deleted table file name,
            for (int i = 0; i < size; ++i) {
                String resource_name = din.readUTF();
                // Convert to new resource type
                if (!resource_name.startsWith(":")) {
                    resource_name = ":1" + resource_name;
                }
                // Add this entry to the delete resource.
                addDeleteResource(new StateResource(-1, resource_name));
            }
            din.close();

        }

        // The sector that contains state information (the table id value)....
        int state_sector = ByteArrayUtil.getInt(reserved_buffer, 8);
        sin = state_file.getSectorInputStream(state_sector);
        din = new DataInputStream(sin);
        din.readInt();   // The version
        int conv_table_id = din.readInt();
        din.close();

        // Close the state file.
        state_file.close();

        // Update the table_id state
        header_area.position(8);
        header_area.putLong(conv_table_id);
        // Check out the change
        header_area.checkOut();

        // Finally commit the changes
        commit();

        // Return a pointer to the structure
        return header_p;
    }

    /**
     * Returns the next table id and increments the table_id counter.
     */
    public synchronized int nextTableID() throws IOException {
        int cur_counter = table_id;
        ++table_id;

        try {
            store.lockForWrite();

            // Update the state in the file
            header_area.position(8);
            header_area.putLong(table_id);
            // Check out the change
            header_area.checkOut();

        } finally {
            store.unlockForWrite();
        }

        return cur_counter;
    }

    /**
     * Returns a list of all table resources that are currently in the visible
     * list.
     */
    public synchronized StateResource[] getVisibleList() {
        return (StateResource[])
                visible_list.toArray(new StateResource[visible_list.size()]);
    }

    /**
     * Returns a list of all table resources that are currently in the deleted
     * list.
     */
    public synchronized StateResource[] getDeleteList() {
        return (StateResource[])
                delete_list.toArray(new StateResource[delete_list.size()]);
    }

    /**
     * Returns true if the visible list contains a state resource with the given
     * table id value.
     */
    public synchronized boolean containsVisibleResource(int table_id) {
        int sz = visible_list.size();
        for (Object o : visible_list) {
            if (((StateResource) o).table_id == table_id) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds the given StateResource to the visible table list.  This does not
     * persist the state.  To persist this change a call to 'commit' must be
     * called.
     */
    public synchronized void addVisibleResource(StateResource resource) {
        visible_list.add(resource);
        vis_list_change = true;
    }

    /**
     * Adds the given StateResource to the deleted table list.  This does not
     * persist the state.  To persist this change a call to 'commit' must be
     * called.
     */
    public synchronized void addDeleteResource(StateResource resource) {
        delete_list.add(resource);
        del_list_change = true;
    }

    /**
     * Removes the resource with the given name from the visible list.  This does
     * not persist the state.  To persist this change a call to 'commit' must be
     * called.
     */
    public synchronized void removeVisibleResource(String name) {
        removeResource(visible_list, name);
        vis_list_change = true;
    }

    /**
     * Removes the resource with the given name from the deleted list.  This does
     * not persist the state.  To persist this change a call to 'commit' must be
     * called.
     */
    public synchronized void removeDeleteResource(String name) {
        removeResource(delete_list, name);
        del_list_change = true;
    }

    /**
     * Commits the current state to disk so that it makes a persistent change to
     * the state.  A further call to 'synch()' will synchronize the file.  This
     * will only commit changes if there were modifications to the state.
     * Returns true if this commit caused any changes to the persistant state.
     */
    public synchronized boolean commit() throws IOException {
        boolean changes = false;
        long new_vis_p = vis_p;
        long new_del_p = del_p;

        try {
            store.lockForWrite();

            // If the lists changed, then write new state areas to the store.
            if (vis_list_change) {
                new_vis_p = writeListToStore(visible_list);
                vis_list_change = false;
                changes = true;
            }
            if (del_list_change) {
                new_del_p = writeListToStore(delete_list);
                del_list_change = false;
                changes = true;
            }
            // Commit the changes,
            if (changes) {
                header_area.position(16);
                header_area.putLong(new_vis_p);
                header_area.putLong(new_del_p);
                // Check out the change.
                header_area.checkOut();
                if (vis_p != new_vis_p) {
                    store.deleteArea(vis_p);
                    vis_p = new_vis_p;
                }
                if (del_p != new_del_p) {
                    store.deleteArea(del_p);
                    del_p = new_del_p;
                }
            }

        } finally {
            store.unlockForWrite();
        }

        return changes;
    }

    // ---------- Inner classes ----------

    /**
     * Represents a single StateResource in either a visible or delete list in
     * this state file.
     */
    static class StateResource {

        /**
         * The unique identifier for the resource.
         */
        final long table_id;

        /**
         * The unique name given to the resource to distinguish it from all other
         * resources.
         */
        final String name;

        public StateResource(long table_id, String name) {
            this.table_id = table_id;
            this.name = name;
        }

    }

}

