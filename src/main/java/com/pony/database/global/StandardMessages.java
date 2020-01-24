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

package com.pony.database.global;

/**
 * This class contains a number of standard messages that are displayed
 * throughout the operation of the database.  They are put into a single class
 * to allow for easy future modification.
 *
 * @author Tobias Downer
 */

public final class StandardMessages {

    /**
     * The name of the author (me).
     */
    public static String AUTHOR = "IllayDevel";

    /**
     * The standard copyright message.
     */
    public static final String COPYRIGHT =
            "Copyright (C) 2019-2020 IllayDevel " +
                    "All rights reserved.";

    /**
     * The global version number of the database system.
     */
    public static final String VERSION = "1.0.6";

    /**
     * The global name of the system.
     */
    public static final String NAME = "Pony SQL Database ( " + VERSION + " )";

}
