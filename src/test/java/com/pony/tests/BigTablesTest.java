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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for the old manual BigTablesTest main program.
 */
class BigTablesTest {

    @TempDir
    Path tempDir;

    @Test
    void storesAndReadsBinaryStreams() throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.executeQuery("CREATE TABLE AccountLog ( " +
                    " id NUMERIC, " +
                    " v1 NUMERIC INDEX_NONE, " +
                    " v2 NUMERIC INDEX_NONE, " +
                    " v3 NUMERIC INDEX_NONE, " +
                    " data BINARY INDEX_NONE )");

            insertBinaryRow(connection, 1, 1024, 17, 11);
            insertBinaryRow(connection, 2, 9 * 1024, 29, 23);
            connection.commit();

            assertBinaryRow(connection, 1, 1024, 17, 11);
            assertBinaryRow(connection, 2, 9 * 1024, 29, 23);
        } finally {
            database.close();
        }
    }

    private void insertBinaryRow(Connection connection, int id, int size,
                                 int param1, int param2) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO AccountLog VALUES ( ?, ?, ?, ?, ? )")) {
            insert.setInt(1, id);
            insert.setInt(2, size);
            insert.setInt(3, param1);
            insert.setInt(4, param2);
            insert.setBinaryStream(
                    5,
                    new LargeAlgorithmicBinaryStream(size, param1, param2),
                    size);
            assertEquals(1, insert.executeUpdate());
        }
    }

    private void assertBinaryRow(Connection connection, int id, int expectedSize,
                                 int expectedParam1, int expectedParam2)
            throws Exception {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT v1, v2, v3, data FROM AccountLog WHERE id = ?")) {
            select.setInt(1, id);
            try (ResultSet result = select.executeQuery()) {
                assertTrue(result.next());
                int size = result.getInt(1);
                int param1 = result.getInt(2);
                int param2 = result.getInt(3);
                assertEquals(expectedSize, size);
                assertEquals(expectedParam1, param1);
                assertEquals(expectedParam2, param2);

                try (InputStream actual = result.getBinaryStream(4);
                     InputStream expected =
                             new LargeAlgorithmicBinaryStream(
                                     size, param1, param2)) {
                    assertEquals(size, compareStreams(actual, expected));
                }
                assertFalse(result.next());
            }
        }
    }

    private long compareStreams(InputStream actual, InputStream expected)
            throws IOException {
        long readCount = 0;
        while (true) {
            int actualValue = actual.read();
            int expectedValue = expected.read();
            assertEquals(expectedValue, actualValue);
            if (actualValue == -1) {
                return readCount;
            }
            ++readCount;
        }
    }

    /**
     * A simple InputStream implementation that iterates across a seeded
     * algorithmic pattern of the given size.
     */
    private static class LargeAlgorithmicBinaryStream extends InputStream {

        private final int maxSize;
        private int index;

        private final int param1;
        private final int param2;

        LargeAlgorithmicBinaryStream(int maxSize, int param1, int param2) {
            this.maxSize = maxSize;
            this.index = 0;
            this.param1 = param1;
            this.param2 = param2;
        }

        public int read() {
            if (index == maxSize) {
                return -1;
            }
            int value = ((index * (param1 + (index / param2))) & 0x0FF);
            ++index;
            return value;
        }
    }

}
