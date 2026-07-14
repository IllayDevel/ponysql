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

package com.pony.tests;

import com.pony.database.control.DBController;
import com.pony.database.control.DBSystem;
import com.pony.database.control.DefaultDBConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression coverage for the old manual ManyTableTest main program.
 */
class ManyTableTest {

    private static final int TABLE_COUNT = 128;

    @TempDir
    Path tempDir;

    @Test
    void createsAndQueriesManyTables() throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            for (int i = 0; i < TABLE_COUNT; ++i) {
                statement.executeQuery(
                        "CREATE TABLE Table" + i +
                                " ( c1 INTEGER, c2 VARCHAR )");
            }

            insertRow(connection, 0, 10, "first");
            insertRow(connection, TABLE_COUNT - 1, 20, "last");

            assertEquals("first", queryValue(connection, 0, 10));
            assertEquals("last", queryValue(connection, TABLE_COUNT - 1, 20));
            assertEquals(TABLE_COUNT, visibleUserTableCount(statement));
        } finally {
            database.close();
        }
    }

    private void insertRow(Connection connection, int tableNumber, int value,
                           String text) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO Table" + tableNumber +
                        " ( c1, c2 ) VALUES ( ?, ? )")) {
            insert.setInt(1, value);
            insert.setString(2, text);
            insert.executeUpdate();
        }
    }

    private String queryValue(Connection connection, int tableNumber, int value)
            throws Exception {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT c2 FROM Table" + tableNumber + " WHERE c1 = ?")) {
            select.setInt(1, value);
            try (ResultSet result = select.executeQuery()) {
                result.next();
                return result.getString(1);
            }
        }
    }

    private int visibleUserTableCount(Statement statement) throws Exception {
        int count = 0;
        try (ResultSet result = statement.executeQuery("SHOW TABLES")) {
            while (result.next()) {
                ++count;
            }
        }
        return count;
    }

}
