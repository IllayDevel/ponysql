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
 * This interface represents a database procedure that is executed on the
 * server side.  It is used to perform database specific functions that can
 * only be performed on the server.
 * <p>
 * A procedure must manage its own table locking.
 *
 * @author Tobias Downer
 */

public interface DatabaseProcedure {

    /**
     * Executes the procudure and returns the resultant table.  Note, the
     * args have to be serializable.  There may be only 0 to 16 arguments.
     * The method may throw a 'DatabaseException' if the procedure failed.
     */
    Table execute(User user, Object[] args) throws DatabaseException;

    /**
     * This returns a DataTable[] array that lists the DataTables that are read
     * during this procedure.
     */
    DataTable[] getReadTables(DatabaseConnection db) throws DatabaseException;

    /**
     * Returns a DataTable[] array that lists the DataTables that are written
     * to during this procedure.
     */
    DataTable[] getWriteTables(DatabaseConnection db) throws DatabaseException;

    /**
     * Returns the locking mode in which the database operates.  This is either
     * LockingMechanism.SHARED_MODE or LockingMechanism.EXCLUSIVE_MODE.  In most
     * cases this will be SHARED_MODE.
     */
    int getLockingMode();

    /**
     * Sets the LockHandle object for this procedure.  This should be called
     * after the tables that this procedure uses have been locked.
     */
    void setLockHandle(LockHandle lock_handle);

}
