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

package com.pony.database.jdbc;

/**
 * An interface that is input to the DatabaseInterface as a way to be
 * notified of event information from inside the database.
 *
 * @author Tobias Downer
 */

public interface DatabaseCallBack {

    /**
     * Called when the database has generated an event that this user is
     * listening for.
     * <p>
     * NOTE: The thread that calls back these events is always a volatile
     *   thread that may not block.  It is especially important that no queries
     *   are executed when this calls back.  To safely act on events, it is
     *   advisable to dispatch onto another thread such as the
     *   SwingEventDispatcher thread.
     */
    void databaseEvent(int event_type, String event_message);

}
