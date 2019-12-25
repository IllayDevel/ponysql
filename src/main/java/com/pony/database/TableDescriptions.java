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

import java.io.*;
import java.util.*;

/**
 * An object that is a key part of Database.  This object maintains a list
 * of descriptions of all tables in the database.  The list contains
 * information about the columns in the table and any other misc table options.
 *
 * @author Tobias Downer
 */

public final class TableDescriptions {

    /**
     * The filename of the file that describes every table in the database.
     */
    private static final String TABLE_DESC_FILE = "PonyDB.desc";

    /**
     * The File that contains the table descriptions list.
     */
    private final File table_desc_file;

    /**
     * The File we use to temporary store the table descriptions as we save
     * them.
     */
    private final File temp_desc_file;

    /**
     * The backup file for table descriptions.
     */
    private final File backup_file;

    /**
     * A hash table that maps from table name to the DataTableDef object that
     * describes the table.
     */
    private HashMap table_descriptions;

    /**
     * Constructs this object with the database in the given directory.
     */
    public TableDescriptions(File database_path) {
        table_desc_file = new File(database_path, TABLE_DESC_FILE);
        temp_desc_file = new File(database_path, TABLE_DESC_FILE + ".temp");
        backup_file = new File(database_path, TABLE_DESC_FILE + ".bak");
        clear();
    }

    /**
     * Returns true if the table descriptions file exists.
     */
    public boolean exists() {
        return table_desc_file.exists() && !table_desc_file.isDirectory();
    }

    /**
     * Load the entire list of table descriptions for this database.
     */
    public void load() throws IOException {

        // Does the table description file exist?
        if (table_desc_file.exists()) {
            // The file exists so load up the table descriptions and put each table
            // in the table_descriptions map.
            DataInputStream din = new DataInputStream(
                    new BufferedInputStream(new FileInputStream(table_desc_file)));

            int ver = din.readInt();
            int table_count = din.readInt();
            for (int i = 0; i < table_count; ++i) {
                DataTableDef table_desc = DataTableDef.read(din);
                String name = table_desc.getName();
                table_descriptions.put(name, table_desc);
            }

            din.close();
        }

    }

    /**
     * Updates the table description file in the database.  The table description
     * file describes every table in the database.  It is loaded when the
     * database is initialized and refreshed whenever a table alteration occurs
     * or the database is shut down.
     */
    public void save() throws IOException {

        DataOutputStream dout = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(temp_desc_file)));

        dout.writeInt(1);
        String[] table_list = getTableList();
        dout.writeInt(table_list.length);
        for (int i = 0; i < table_list.length; ++i) {
            // Write the DataTableDef for this table
            ((DataTableDef) table_descriptions.get(table_list[i])).write(dout);
        }

        dout.flush();
        dout.close();

        // Delete the current backup file and rename the temp file to the official
        // file.
        // Cycle through the backups...

        backup_file.delete();
        table_desc_file.renameTo(backup_file);
        temp_desc_file.renameTo(table_desc_file);

    }

    /**
     * Adds a new DataTableDef object to the list of tables in the database.
     */
    void add(DataTableDef table) throws IOException {
        table_descriptions.put(table.getName(), table);
    }

    /**
     * Removes a DataTableDef object from the list with the given name.
     */
    void remove(String name) throws IOException {
        table_descriptions.remove(name);
    }

    /**
     * Returns a list of table name's sorted in alphebetical order.
     */
    public String[] getTableList() {
        Set keys = table_descriptions.keySet();
        String[] all_keys = (String[]) keys.toArray(new String[keys.size()]);
        Arrays.sort(all_keys);
        return all_keys;
    }

    /**
     * Clears this object completely.
     */
    void clear() {
        table_descriptions = new HashMap(150, 0.50f);
    }

    /**
     * Returns the DataTableDef object for the table with the given name.  The
     * description must have been loaded before this method is called.  Returns
     * null if the table was not found.
     */
    public DataTableDef getDef(String table_name) {
        return (DataTableDef) table_descriptions.get(table_name);
    }


}
