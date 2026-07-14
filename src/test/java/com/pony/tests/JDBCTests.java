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
 * Regression coverage for the old manual JDBCTests main program.
 */
class JDBCTests {

    @TempDir
    Path tempDir;

    @Test
    void jdbcConnectionsMetadataUpdatesAndOrderingWorkTogether()
            throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Connection connection1 = database.getConnection("test", "test");
             Connection connection2 = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertNotNull(metaData.getDatabaseProductName());
            assertNotNull(metaData.getDatabaseProductVersion());

            connection.setAutoCommit(false);
            connection1.setAutoCommit(false);
            connection2.setAutoCommit(false);

            createJdbcTestTable(statement, "Test1");
            createJdbcTestTable(statement, "Test2");
            statement.executeUpdate(
                    "CREATE INDEX idx_jdbc_test1_rand ON Test1(rand_val)");
            statement.executeUpdate(
                    "CREATE INDEX idx_jdbc_test2_rand ON Test2(rand_val)");
            connection.commit();

            insertRows(connection1, "Test1", "T1", 64, 700);
            connection1.commit();
            insertRows(connection2, "Test2", "T2", 48, 500);
            connection2.commit();

            assertEquals(64, queryCount(connection,
                    "SELECT COUNT(*) FROM Test1"));
            assertEquals(48, queryCount(connection,
                    "SELECT COUNT(*) FROM Test2"));

            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE Test1 " +
                            "SET string_col = CONCAT(string_col, '--updated') " +
                            "WHERE string_col = ?")) {
                update.setString(1, "A test string!");
                assertEquals(64, update.executeUpdate());
            }
            connection.commit();

            assertEquals(64, queryCount(connection,
                    "SELECT COUNT(*) FROM Test1 " +
                            "WHERE string_col = 'A test string!--updated'"));
            assertOrderedRandValues(connection);
            assertShowTables(statement, List.of("Test1", "Test2"));
        } finally {
            database.close();
        }
    }

    private void createJdbcTestTable(Statement statement, String table)
            throws Exception {
        statement.executeUpdate(
                "CREATE TABLE " + table + " ( " +
                        " id VARCHAR(200), " +
                        " rand_val INTEGER, " +
                        " binary_col BINARY, " +
                        " string_col VARCHAR(1048576) )");
    }

    private void insertRows(Connection connection, String table, String prefix,
                            int count, int modulus) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + table +
                        " ( id, rand_val, binary_col, string_col ) " +
                        "VALUES ( ?, ?, ?, ? )")) {
            for (int i = 0; i < count; ++i) {
                insert.setString(1, prefix + "-" + i);
                insert.setInt(2, (i * 37) % modulus);
                insert.setBytes(3, new byte[]{(byte) i, (byte) (i * 2)});
                insert.setString(4, "A test string!");
                assertEquals(1, insert.executeUpdate());
            }
        }
    }

    private int queryCount(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            int count = result.getInt(1);
            assertFalse(result.next());
            return count;
        }
    }

    private void assertOrderedRandValues(Connection connection)
            throws Exception {
        int previous = Integer.MIN_VALUE;
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT rand_val, string_col FROM Test1 " +
                             "WHERE string_col = 'A test string!--updated' " +
                             "ORDER BY rand_val")) {
            int count = 0;
            while (result.next()) {
                int value = result.getInt(1);
                assertTrue(value >= previous);
                assertEquals("A test string!--updated", result.getString(2));
                previous = value;
                ++count;
            }
            assertEquals(64, count);
        }
    }

    private void assertShowTables(Statement statement, List<String> names)
            throws Exception {
        List<String> visibleTables = new ArrayList<>();
        try (ResultSet result = statement.executeQuery("SHOW TABLES")) {
            while (result.next()) {
                visibleTables.add(result.getString(1));
            }
        }
        for (String name : names) {
            assertTrue(visibleTables.contains(name));
        }
    }

}
