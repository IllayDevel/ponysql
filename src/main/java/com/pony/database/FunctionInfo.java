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
 * Meta information about a function.  Used to compile information about a
 * particular function.
 *
 * @author Tobias Downer
 */

public interface FunctionInfo {

    /**
     * A type that represents a static function.  A static function is not
     * an aggregate therefore does not require a GroupResolver.  The result of
     * a static function is guarenteed the same given identical parameters over
     * subsequent calls.
     */
    int STATIC = 1;

    /**
     * A type that represents an aggregate function.  An aggregate function
     * requires the GroupResolver variable to be present in able to resolve the
     * function over some set.  The result of an aggregate function is
     * guarenteed the same given the same set and identical parameters.
     */
    int AGGREGATE = 2;

    /**
     * A function that is non-aggregate but whose return value is not guarenteed
     * to be the same given the identical parameters over subsequent calls.  This
     * would include functions such as RANDOM and UNIQUEKEY.  The result is
     * dependant on some other state (a random seed and a sequence value).
     */
    int STATE_BASED = 3;


    /**
     * The name of the function as used by the SQL grammar to reference it.
     */
    String getName();

    /**
     * The type of function, either STATIC, AGGREGATE or STATE_BASED (eg. result
     * is not dependant entirely from input but from another state for example
     * RANDOM and UNIQUEKEY functions).
     */
    int getType();

    /**
     * The name of the function factory class that this function is handled by.
     * For example, "com.pony.database.InternalFunctionFactory".
     */
    String getFunctionFactoryName();

}
