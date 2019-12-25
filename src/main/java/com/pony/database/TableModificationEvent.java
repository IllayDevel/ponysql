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

/**
 * The event information of when a table is modified inside a transaction.
 *
 * @author Tobias Downer
 */

public class TableModificationEvent {

    // ----- Statics -----

    /**
     * Event that occurs before the action
     */
    public static final int BEFORE = 0x010;

    /**
     * Event that occurs after the action
     */
    public static final int AFTER = 0x020;

    // ---

    /**
     * Event type for insert action.
     */
    public static final int INSERT = 0x001;

    /**
     * Event type for update action.
     */
    public static final int UPDATE = 0x002;

    /**
     * Event type for delete action.
     */
    public static final int DELETE = 0x004;

    // ---

    /**
     * Event for before an insert.
     */
    public static final int BEFORE_INSERT = BEFORE | INSERT;

    /**
     * Event for after an insert.
     */
    public static final int AFTER_INSERT = AFTER | INSERT;

    /**
     * Event for before an update.
     */
    public static final int BEFORE_UPDATE = BEFORE | UPDATE;

    /**
     * Event for after an update.
     */
    public static final int AFTER_UPDATE = AFTER | UPDATE;

    /**
     * Event for before a delete.
     */
    public static final int BEFORE_DELETE = BEFORE | DELETE;

    /**
     * Event for after a delete.
     */
    public static final int AFTER_DELETE = AFTER | DELETE;


    // ----- Members -----

    /**
     * The DatabaseConnection of the table that the modification occurred in.
     */
    private DatabaseConnection connection;

    /**
     * The name of the table that was modified.
     */
    private TableName table_name;

    /**
     * The type of event that occurred.
     */
    private int event_type;

    /**
     * A RowData object representing the row that is being inserted by this
     * modification.  This is set for INSERT and UPDATE events.  If the event
     * type is BEFORE then this data represents the new data in the table and
     * can be modified.  This represents the NEW information.
     */
    private RowData row_data;

    /**
     * The row index of the table that is before removed by this modification.
     * This is set for UPDATE and DELETE events.  This represents the OLD
     * information.
     */
    private int row_index = -1;

    /**
     * General Constructor.
     */
    private TableModificationEvent(DatabaseConnection connection,
                                   TableName table_name, int row_index, RowData row_data,
                                   int type, boolean before) {
        this.connection = connection;
        this.table_name = table_name;
        this.row_index = row_index;
        this.row_data = row_data;
        this.event_type = type | (before ? BEFORE : AFTER);
    }

    /**
     * Constructs an insert event.
     */
    TableModificationEvent(DatabaseConnection connection, TableName table_name,
                           RowData row_data, boolean before) {
        this(connection, table_name, -1, row_data, INSERT, before);
    }

    /**
     * Constructs an update event.
     */
    TableModificationEvent(DatabaseConnection connection, TableName table_name,
                           int row_index, RowData row_data, boolean before) {
        this(connection, table_name, row_index, row_data, UPDATE, before);
    }

    /**
     * Constructs a delete event.
     */
    TableModificationEvent(DatabaseConnection connection, TableName table_name,
                           int row_index, boolean before) {
        this(connection, table_name, row_index, null, DELETE, before);
    }

    /**
     * Returns the DatabaseConnection that this event fired in.
     */
    public DatabaseConnection getDatabaseConnection() {
        return connection;
    }

    /**
     * Returns the event type.
     */
    public int getType() {
        return event_type;
    }

    /**
     * Returns true if this is a BEFORE event.
     */
    public boolean isBefore() {
        return (event_type & BEFORE) != 0;
    }

    /**
     * Returns true if this is a AFTER event.
     */
    public boolean isAfter() {
        return (event_type & AFTER) != 0;
    }

    /**
     * Returns true if this is an INSERT event.
     */
    public boolean isInsert() {
        return (event_type & INSERT) != 0;
    }

    /**
     * Returns true if this is an UPDATE event.
     */
    public boolean isUpdate() {
        return (event_type & UPDATE) != 0;
    }

    /**
     * Returns true if this is an DELETE event.
     */
    public boolean isDelete() {
        return (event_type & DELETE) != 0;
    }

    /**
     * Returns the name of the table of this modification.
     */
    public TableName getTableName() {
        return table_name;
    }

    /**
     * Returns the index of the row in the table that was affected by this
     * event or -1 if event type is INSERT.
     */
    public int getRowIndex() {
        return row_index;
    }

    /**
     * Returns the RowData object that represents the change that is being
     * made to the table either by an INSERT or UPDATE.  For a DELETE event this
     * return null.
     */
    public RowData getRowData() {
        return row_data;
    }

    /**
     * Returns true if the given listener type should be notified of this type
     * of table modification event.  For example, if this is a BEFORE event then
     * the BEFORE bit on the given type must be set and if this is an INSERT event
     * then the INSERT bit on the given type must be set.
     */
    public boolean listenedBy(int listen_t) {
        // If this is a BEFORE trigger, then we must be listening for BEFORE events,
        // etc.
        boolean ba_match =
                ((event_type & BEFORE) != 0 && (listen_t & BEFORE) != 0) ||
                        ((event_type & AFTER) != 0 && (listen_t & AFTER) != 0);
        // If this is an INSERT trigger, then we must be listening for INSERT
        // events, etc.
        boolean trig_match =
                ((event_type & INSERT) != 0 && (listen_t & INSERT) != 0) ||
                        ((event_type & DELETE) != 0 && (listen_t & DELETE) != 0) ||
                        ((event_type & UPDATE) != 0 && (listen_t & UPDATE) != 0);
        // If both of the above are true
        return (ba_match && trig_match);
    }

}

