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

package com.pony.database.jdbc;

/**
 * A listener that is notified when the trigger being listened to is fired.
 *
 * @author Tobias Downer
 */

public interface TriggerListener {

    /**
     * Notifies this listener that the trigger with the name has been fired.
     * Trigger's are specified via the SQL syntax and a trigger listener can
     * be registered via PonyConnection.
     *
     * @param trigger_name the name of the trigger that fired.
     */
    void triggerFired(String trigger_name);

}
