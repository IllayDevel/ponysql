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

package com.pony.database;

/**
 * A trigger event represents a high level action that occured in the
 * database.  A trigger event is generated by the SQL interpreter on evaluation
 * of curtain types of queries.
 *
 * @author Tobias Downer
 */

public class TriggerEvent {

    /**
     * Statics that represent the different types of high layer trigger events.
     */
    public static final int INSERT = 1;
    public static final int DELETE = 2;
    public static final int UPDATE = 3;

    /**
     * The type of this event.
     */
    private final int type;

    /**
     * The source of the trigger (eg. the table name).
     */
    private final String source;

    /**
     * The number of times this event was fired.
     */
    private final int count;

    /**
     * Constructs the trigger event.
     */
    public TriggerEvent(int type, String source, int count) {
        this.type = type;
        this.source = source;
        this.count = count;
    }

    public TriggerEvent(int type, String source) {
        this(type, source, 1);
    }

    /**
     * Returns the type of this event.
     */
    public int getType() {
        return type;
    }

    /**
     * Returns the source of this event.
     */
    public String getSource() {
        return source;
    }

    /**
     * Returns the number of times this event was fired.
     */
    public int getCount() {
        return count;
    }

}
