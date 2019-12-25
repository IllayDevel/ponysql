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

package com.pony.database.interpret;

import java.util.ArrayList;

import com.pony.database.*;

/**
 * Represents an action in an ALTER TABLE SQL statement.
 *
 * @author Tobias Downer
 */

public final class AlterTableAction
        implements java.io.Serializable, StatementTreeObject, Cloneable {

    static final long serialVersionUID = -3180332341627416727L;

    /**
     * Element parameters to do with the action.
     */
    private ArrayList<Object> elements;

    /**
     * The action to perform.
     */
    private String action;

    /**
     * Constructor.
     */
    public AlterTableAction() {
        elements = new ArrayList<>();
    }

    /**
     * Set the action to perform.
     */
    public void setAction(String str) {
        this.action = str;
    }

    /**
     * Adds a parameter to this action.
     */
    public void addElement(Object ob) {
        elements.add(ob);
    }

    /**
     * Returns the name of this action.
     */
    public String getAction() {
        return action;
    }

    /**
     * Returns the ArrayList that represents the parameters of this action.
     */
    public ArrayList<Object> getElements() {
        return elements;
    }

    /**
     * Returns element 'n'.
     */
    public Object getElement(int n) {
        return elements.get(n);
    }

    // Implemented from StatementTreeObject
    public void prepareExpressions(ExpressionPreparer preparer)
            throws DatabaseException {
        // This must search throw 'elements' for objects that we can prepare
        for (Object ob : elements) {
            if (ob instanceof String) {
                // Do not need to prepare this
            } else if (ob instanceof Expression) {
                ((Expression) ob).prepare(preparer);
            } else if (ob instanceof StatementTreeObject) {
                ((StatementTreeObject) ob).prepareExpressions(preparer);
            } else {
                throw new DatabaseException(
                        "Unrecognised expression: " + ob.getClass());
            }
        }
    }

    public Object clone() throws CloneNotSupportedException {
        // Shallow clone
        AlterTableAction v = (AlterTableAction) super.clone();
        ArrayList<Object> cloned_elements = new ArrayList<>();
        v.elements = cloned_elements;

        for (Object ob : elements) {
            if (ob instanceof String) {
                // Do not need to clone this
            } else if (ob instanceof Expression) {
                ob = ((Expression) ob).clone();
            } else if (ob instanceof StatementTreeObject) {
                ob = ((StatementTreeObject) ob).clone();
            } else {
                throw new CloneNotSupportedException(ob.getClass().toString());
            }
            cloned_elements.add(ob);
        }

        return v;
    }

}
