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

package com.pony.database.global;

/**
 * A Null Object.
 *
 * @author Tobias Downer
 * @deprecated do not use.  Nulls are now handled via TObject and TType.  This
 *   method is only kept around for legacy with older databases.
 */

public class NullObject implements java.io.Serializable {

    static final long serialVersionUID = 8599490526855696529L;

    public static NullObject NULL_OBJ = new NullObject();

    public int compareTo(Object ob) {
        if (ob == null || ob instanceof NullObject) {
            return 0;
        }
        return -1;
    }

    public String toString() {
        return "NULL";
    }

}
