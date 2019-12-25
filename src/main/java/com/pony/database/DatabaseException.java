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
 * Exception thrown where various problems occur within the database.
 * <p>
 * @author Tobias Downer
 */

public class DatabaseException extends Exception {

    private final int error_code;

    // ---------- Members ----------

    public DatabaseException(int error_code, String message) {
        super(message);
        this.error_code = error_code;
    }

    public DatabaseException(String message) {
        this(-1, message);
    }

    /**
     * Returns the error code.  -1 means no error code was given.
     */
    public int getErrorCode() {
        return error_code;
    }


}
