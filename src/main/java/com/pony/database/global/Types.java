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
 * The possible types used in the database.
 * <p>
 * @author Tobias Downer
 */

public interface Types {

    int DB_UNKNOWN = -1;

    int DB_STRING = 1;
    int DB_NUMERIC = 2;
    int DB_TIME = 3;
    int DB_BINARY = 4;    // @deprecated - use BLOB
    int DB_BOOLEAN = 5;
    int DB_BLOB = 6;
    int DB_OBJECT = 7;

    // This is an extended numeric type that handles neg and positive infinity
    // and NaN.
    int DB_NUMERIC_EXTENDED = 8;

}
