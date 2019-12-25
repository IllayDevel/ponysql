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

import java.util.*;

import com.pony.util.HashMapList;

/**
 * An object that manages high level trigger events within a Database context.
 * This manager is designed to manage the map between session and triggers
 * being listened for.  It is the responsibility of the language parsing
 * layer to notify this manager of trigger events.
 * <p>
 * NOTE: It is intended that this object manages events from the highest layer,
 *   so it is possible that trigger events may not get to be notified if
 *   queries are not evaluated properly.
 * <p>
 * NOTE: This object serves a different purpose than DataTableListener.
 *   DataTableListener is guarenteed to pick up all low level access to the
 *   tables.  This object is only intended as a helper for implementing a
 *   trigger event dispatcher by a higher level package (eg.
 *   com.pony.database.sql)
 * <p>
 * CONCURRENCY: This class is thread safe.  It may safely be accessed by
 *   multiple threads.  Any events that are fired are put on the
 *   DatabaseDispatcher thread.
 *
 * @author Tobias Downer
 */

final class TriggerManager {

    /**
     * The parent TransactionSystem object.
     */
    private final TransactionSystem system;

    /**
     * Maps from the user session (User) to the list of TriggerAction objects
     * for this user.
     */
    private final HashMapList listener_map;

    /**
     * Maps from the trigger source string to the list of TriggerAction
     * objects that are listening for events from this source.
     */
    private final HashMapList table_map;

    /**
     * Constructor.
     */
    TriggerManager(TransactionSystem system) {
        this.system = system;
        listener_map = new HashMapList();
        table_map = new HashMapList();
    }

    /**
     * Flushes the list of TriggerEvent objects and dispatches them to the
     * users that are listening.  This is called after the given connection
     * has successfully committed and closed.
     */
    void flushTriggerEvents(final ArrayList event_list) {
        for (Object o : event_list) {
            TriggerEvent evt = (TriggerEvent) o;
            fireTrigger(evt);
        }
    }

    /**
     * Adds a listener for an event with the given 'id' for this user session.
     * <p>
     * For example,<br>
     *   addTriggerListener(user, "my_trigger",
     *                      TriggerEvent.UPDATE, "Part", my_listener);
     * <p>
     * This listener is notified of all update events on the 'Part' table.
     */
    synchronized void addTriggerListener(DatabaseConnection database,
                                         String trigger_name, int event_id, String trigger_source,
                                         TriggerListener listener) {

        // Has this trigger name already been defined for this user?
        List list = listener_map.get(database);
        for (Object o : list) {
            TriggerAction action = (TriggerAction) o;
            if (action.getName().equals(trigger_name)) {
                throw new Error("Duplicate trigger name '" + trigger_name + "'");
            }
        }

        TriggerAction action = new TriggerAction(database, trigger_name, event_id,
                trigger_source, listener);

        listener_map.put(database, action);
        table_map.put(trigger_source, action);
    }

    /**
     * Removes a trigger for the given user session.
     */
    synchronized void removeTriggerListener(DatabaseConnection database,
                                            String trigger_name) {
        List list = listener_map.get(database);
        for (Object o : list) {
            TriggerAction action = (TriggerAction) o;
            if (action.getName().equals(trigger_name)) {
                listener_map.remove(database, action);
                table_map.remove(action.trigger_source, action);
                return;
            }
        }
        throw new Error("Trigger name '" + trigger_name + "' not found.");
    }

    /**
     * Clears all the user triggers that have been defined.
     */
    synchronized void clearAllDatabaseConnectionTriggers(
            DatabaseConnection database) {
        List list = listener_map.clear(database);
        for (Object o : list) {
            TriggerAction action = (TriggerAction) o;
            table_map.remove(action.trigger_source, action);
        }
    }

    /**
     * Notifies all the listeners on a trigger_source (ie. a table) that a
     * specific type of event has happened, as denoted by the type.
     *
     */
    private void fireTrigger(final TriggerEvent evt) {

        final ArrayList trig_list;
        // Get all the triggers for this trigger source,
//    System.out.println(evt.getSource());
//    System.out.println(table_map);
        synchronized (this) {
            List list = table_map.get(evt.getSource());
            if (list.size() == 0) {
                return;
            }
            trig_list = new ArrayList(list);
        }

        // Post an event that fires the triggers for each listener.
        Runnable runner = () -> {
            for (Object o : trig_list) {
                TriggerAction action = (TriggerAction) o;
                if (evt.getType() == action.trigger_event) {
                    action.listener.fireTrigger(action.database, action.trigger_name,
                            evt);
                }
            }
        };

        // Post the event to go off approx 3ms from now.
        system.postEvent(3, system.createEvent(runner));

    }

    // ---------- Inner classes ----------

    /**
     * Encapsulates the information of a trigger listener for a specific event
     * for a user.
     */
    private static class TriggerAction {

        private final DatabaseConnection database;
        private final String trigger_name;   // The name of the trigger.
        private final TriggerListener listener;       // The trigger listener.
        private final String trigger_source; // The source of the trigger.
        private final int trigger_event;  // Event we are to listen for.

        /**
         * Constructor.
         */
        TriggerAction(DatabaseConnection database, String name, int type,
                      String trigger_source, TriggerListener listener) {
            this.database = database;
            this.trigger_name = name;
            this.trigger_event = type;
            this.listener = listener;
            this.trigger_source = trigger_source;
        }

        /**
         * Returns the name of the trigger.
         */
        public String getName() {
            return trigger_name;
        }

    }

}
