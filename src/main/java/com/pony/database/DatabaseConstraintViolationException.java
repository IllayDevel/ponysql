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
 * A database exception that represents a constraint violation.
 *
 * @author Tobias Downer
 */

public class DatabaseConstraintViolationException extends RuntimeException {

    // ---------- Statics ----------

    /**
     * A Primary Key constraint violation error code.
     */
    public static final int PRIMARY_KEY_VIOLATION = 20;

    /**
     * A Unique constraint violation error code.
     */
    public static final int UNIQUE_VIOLATION = 21;

    /**
     * A Check constraint violation error code.
     */
    public static final int CHECK_VIOLATION = 22;

    /**
     * A Foreign Key constraint violation error code.
     */
    public static final int FOREIGN_KEY_VIOLATION = 23;

    /**
     * A Nullable constraint violation error code (data added to not null
     * columns that was null).
     */
    public static final int NULLABLE_VIOLATION = 24;

    /**
     * Java type constraint violation error code (tried to insert a Java object
     * that wasn't derived from the java object type defined for the column).
     */
    public static final int JAVA_TYPE_VIOLATION = 25;

    /**
     * Tried to drop a table that is referenced by another source.
     */
    public static final int DROP_TABLE_VIOLATION = 26;

    /**
     * Column can't be dropped before of an reference to it.
     */
    public static final int DROP_COLUMN_VIOLATION = 27;


    /**
     * The error code.
     */
    private int error_code;

    /**
     * Constructor.
     */
    public DatabaseConstraintViolationException(int err_code, String msg) {
        super(msg);
        this.error_code = err_code;
    }

    /**
     * Returns the violation error code.
     */
    public int getErrorCode() {
        return error_code;
    }

}
