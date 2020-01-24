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
 * An interface used to prepare an Expression object.  This interface is used
 * to mutate an element of an Expression from one form to another.  For
 * example, we may use this to translate a StatementTree object to a
 * Statement object.
 *
 * @author Tobias Downer
 */

public interface ExpressionPreparer {

    /**
     * Returns true if this preparer will prepare the given object in an
     * expression.
     */
    boolean canPrepare(Object element);

    /**
     * Returns the new translated object to be mutated from the given element.
     */
    Object prepare(Object element) throws DatabaseException;

}
