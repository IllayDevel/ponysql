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
 * An object that represents a constant value that is to be lately binded to
 * a constant value in an Expression.  This is used when we have ? style
 * prepared statement values.  This object is used as a marker in the
 * elements of a expression.
 *
 * @author Tobias Downer
 */

public class ParameterSubstitution implements java.io.Serializable {

    static final long serialVersionUID = -740886588230246432L;

    /**
     * The numerical number of this parameter substitution.  The first
     * substitution is '0', the second is '1', etc.
     */
    private int parameter_id;

    /**
     * Creates the substitution.
     */
    public ParameterSubstitution(int parameter_id) {
        this.parameter_id = parameter_id;
    }

    /**
     * Returns the number of this parameter id.
     */
    public int getID() {
        return parameter_id;
    }

    /**
     * Equality test.
     */
    public boolean equals(Object ob) {
        ParameterSubstitution sub = (ParameterSubstitution) ob;
        return this.parameter_id == sub.parameter_id;
    }

}
