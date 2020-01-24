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
 * An interface that resolves and generates a Function objects given a
 * FunctionDef object.
 *
 * @author Tobias Downer
 */

public interface FunctionLookup {

    /**
     * Generate the Function given a FunctionDef object.  Returns null if the
     * FunctionDef can not be resolved to a valid function object.  If the
     * specification of the function is invalid for some reason (the number or
     * type of the parameters is incorrect) then a StatementException is thrown.
     */
    Function generateFunction(FunctionDef function_def);

    /**
     * Returns true if the function defined by FunctionDef is an aggregate
     * function, or false otherwise.
     */
    boolean isAggregate(FunctionDef function_def);

}
