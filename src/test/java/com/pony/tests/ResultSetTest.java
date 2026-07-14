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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for the old manual ResultSetTest main program.
 */
class ResultSetTest {

    @TempDir
    Path tempDir;

    @Test
    void preparedStatementsExposeResultSetsForUpdatesAndQueries()
            throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertNotNull(metaData.getDatabaseProductName());
            assertNotNull(metaData.getDatabaseProductVersion());

            statement.executeQuery("DROP TABLE IF EXISTS RSTest");
            statement.executeQuery(
                    "CREATE TABLE IF NOT EXISTS RSTest ( " +
                            " cola VARCHAR(60), " +
                            " colb VARCHAR(60), " +
                            " colc INTEGER, " +
                            " cold NUMERIC, " +
                            " cole BOOLEAN )");

            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO RSTest ( cola, colb, colc, cold, cole ) " +
                            "VALUES ( ?, ?, ?, ?, ? )")) {
                for (int value : new int[]{5, 6, 7, 8}) {
                    insert.setString(1, "Bah1");
                    insert.setString(2, "Bah2");
                    insert.setInt(3, value);
                    insert.setDouble(4, 90.55);
                    insert.setBoolean(5, true);
                    assertEquals(1, insert.executeUpdate());
                }
            }

            try (PreparedStatement delete = connection.prepareStatement(
                    "DELETE FROM RSTest WHERE colc = ?")) {
                assertUpdateCount(delete, 6, 1);
                assertUpdateCount(delete, 7, 1);
                assertUpdateCount(delete, 10, 0);
            }

            assertEquals(List.of(5, 8), remainingColcValues(connection));
            assertHasRows(statement, "SHOW STATUS");
            assertHasRows(statement, "SHOW CONNECTIONS");
        } finally {
            database.close();
        }
    }

    private void assertUpdateCount(PreparedStatement statement,
                                   int value, int expectedCount)
            throws Exception {
        statement.setInt(1, value);
        try (ResultSet result = statement.executeQuery()) {
            assertTrue(result.next());
            assertEquals(expectedCount, result.getInt(1));
            assertFalse(result.next());
        }
    }

    private List<Integer> remainingColcValues(Connection connection)
            throws Exception {
        List<Integer> values = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT colc FROM RSTest ORDER BY colc")) {
            while (result.next()) {
                values.add(result.getInt(1));
            }
        }
        return values;
    }

    private void assertHasRows(Statement statement, String sql) throws Exception {
        try (ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
        }
    }

}
