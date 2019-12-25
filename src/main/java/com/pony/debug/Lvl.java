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

package com.pony.debug;

/**
 * Debug level static values.
 *
 * @author Tobias Downer
 */

public interface Lvl {

    /**
     * Some sample debug levels.
     */
    int INFORMATION = 10;    // General processing 'noise'
    int WARNING = 20;    // A message of some importance
    int ALERT = 30;    // Crackers, etc
    int ERROR = 40;    // Errors, exceptions
    int MESSAGE = 10000; // Always printed messages
    // (not error's however)

}
