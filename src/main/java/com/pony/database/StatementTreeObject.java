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
 * An complex object that is to be contained within a StatementTree object.
 * A statement tree object must be serializable, and it must be able to
 * reference all Expression objects so that they may be prepared.
 *
 * @author Tobias Downer
 */

public interface StatementTreeObject {

    /**
     * Prepares all expressions in this statement tree object by passing the
     * ExpressionPreparer object to the 'prepare' method of the expression.
     */
    void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException;

    /**
     * Performs a DEEP clone of this object if it is mutable, or a deep clone
     * of its mutable members.  If the object is immutable then it may return
     * 'this'.
     */
    Object clone() throws CloneNotSupportedException;

}
