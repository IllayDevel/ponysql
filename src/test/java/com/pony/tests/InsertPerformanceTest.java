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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for the old manual InsertPerformanceTest main program.
 */
class InsertPerformanceTest {

    private static final int INSERT_COUNT = 512;

    @TempDir
    Path tempDir;

    @Test
    void insertsPreparedRowsAcrossCommitBatches() throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeQuery(
                    "CREATE TABLE TTable (" +
                            " c1 NUMERIC, c2 NUMERIC, c3 TEXT, c4 TEXT )");

            connection.setAutoCommit(false);
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO TTable ( c1, c2, c3, c4 ) " +
                            "VALUES ( ?, ?, ?, ? )")) {
                for (int i = 0; i < INSERT_COUNT; ++i) {
                    insert.setInt(1, i);
                    insert.setInt(2, i + 10000);
                    insert.setString(3, i + " - a string");
                    insert.setString(4, "This is some data being inserted.");
                    assertEquals(1, insert.executeUpdate());

                    if ((i % 128) == 127) {
                        connection.commit();
                    }
                }
            }
            connection.commit();

            try (ResultSet result = statement.executeQuery(
                    "SELECT COUNT(*), MIN(c1), MAX(c2) FROM TTable")) {
                assertTrue(result.next());
                assertEquals(INSERT_COUNT, result.getInt(1));
                assertEquals(0, result.getInt(2));
                assertEquals(10000 + INSERT_COUNT - 1, result.getInt(3));
                assertFalse(result.next());
            }
        } finally {
            database.close();
        }
    }

}
