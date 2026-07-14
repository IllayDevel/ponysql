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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for the old manual PerformTest1 main program.
 */
class PerformSortTest {

    private static final int ROW_COUNT = 1024;

    @TempDir
    Path tempDir;

    @Test
    void repeatedlySortsInsertedRowsByMultipleColumns() throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.executeUpdate(
                    "CREATE TABLE perform_table ( " +
                            " c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER )");
            connection.commit();

            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO perform_table VALUES ( ?, ?, ?, ? )")) {
                for (int i = 0; i < ROW_COUNT; ++i) {
                    insert.setInt(1, i);
                    insert.setInt(2, -i);
                    insert.setInt(3, (i * 5) / 20);
                    insert.setInt(4, i / 90);
                    assertEquals(1, insert.executeUpdate());
                    if ((i % 256) == 255) {
                        connection.commit();
                    }
                }
            }
            connection.commit();

            for (int i = 0; i < 5; ++i) {
                assertSortedRows(statement);
            }
        } finally {
            database.close();
        }
    }

    private void assertSortedRows(Statement statement) throws Exception {
        int count = 0;
        int previousC2 = Integer.MIN_VALUE;
        int previousC3 = Integer.MIN_VALUE;
        int previousC4 = Integer.MIN_VALUE;

        try (ResultSet result = statement.executeQuery(
                "SELECT c1, c2, c3, c4 FROM perform_table " +
                        "ORDER BY c4, c3, c2")) {
            while (result.next()) {
                int c1 = result.getInt(1);
                int c2 = result.getInt(2);
                int c3 = result.getInt(3);
                int c4 = result.getInt(4);

                assertEquals(-c1, c2);
                assertEquals((c1 * 5) / 20, c3);
                assertEquals(c1 / 90, c4);
                if (count > 0) {
                    assertTrue(compareTuple(previousC4, previousC3, previousC2,
                            c4, c3, c2) <= 0);
                }

                previousC2 = c2;
                previousC3 = c3;
                previousC4 = c4;
                ++count;
            }
        }

        assertEquals(ROW_COUNT, count);
    }

    private int compareTuple(int leftC4, int leftC3, int leftC2,
                             int rightC4, int rightC3, int rightC2) {
        if (leftC4 != rightC4) {
            return Integer.compare(leftC4, rightC4);
        }
        if (leftC3 != rightC3) {
            return Integer.compare(leftC3, rightC3);
        }
        return Integer.compare(leftC2, rightC2);
    }

}
