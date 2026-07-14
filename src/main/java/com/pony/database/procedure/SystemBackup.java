/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

package com.pony.database.procedure;

import com.pony.database.ProcedureConnection;
import com.pony.database.ProcedureException;

import java.io.File;
import java.io.IOException;

/**
 * A stored procedure that backs up the entire database to the given directory
 * in the file system.  Requires one parameter, the locate to back up the
 * database to.
 *
 * @author Tobias Downer
 */

public class SystemBackup {

    /**
     * The stored procedure invokation method.
     */
    public static String invoke(ProcedureConnection db_connection,
                                String path) {

        File f = new File(path);
        if (!f.exists() || !f.isDirectory()) {
            throw new ProcedureException("Path '" + path +
                    "' doesn't exist or is not a directory.");
        }

        try {
            db_connection.getDatabase().liveCopyTo(f);
            return path;
        } catch (IOException e) {
            e.printStackTrace();
            throw new ProcedureException("IO Error: " + e.getMessage());
        }

    }

}

