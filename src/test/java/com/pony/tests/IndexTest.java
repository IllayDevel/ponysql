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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IndexTest {

    @TempDir
    Path tempDir;

    @Test
    void createIndexIsNotNoOpAndValidatesDefinitions() throws Exception {
        DBSystem database = createDatabase(true);
        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE idx_test ( id INTEGER, group_id INTEGER )");
            statement.executeUpdate(
                    "CREATE INDEX idx_group ON idx_test(group_id)");

            assertThrows(SQLException.class, () -> statement.executeUpdate(
                    "CREATE INDEX idx_group ON idx_test(id)"));
            assertThrows(SQLException.class, () -> statement.executeUpdate(
                    "CREATE INDEX idx_missing ON idx_test(missing_col)"));
        } finally {
            database.close();
        }
    }

    @Test
    void compositeIndexSupportsEqualityLookupAndStaysCurrent() throws Exception {
        DBSystem database = createDatabase(true);
        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE composite_test (" +
                            " id INTEGER, group_id INTEGER, bucket INTEGER, " +
                            " payload VARCHAR(64) )");
            insertRow(connection, 1, 10, 1, "a");
            insertRow(connection, 2, 10, 2, "b");
            insertRow(connection, 3, 20, 1, "c");

            statement.executeUpdate(
                    "CREATE INDEX idx_group_bucket " +
                            "ON composite_test(group_id, bucket)");

            assertEquals(List.of("b"), queryPayloads(connection,
                    "SELECT payload FROM composite_test " +
                            "WHERE group_id = 10 AND bucket = 2"));

            insertRow(connection, 4, 10, 2, "d");
            assertEquals(List.of("b", "d"), queryPayloads(connection,
                    "SELECT payload FROM composite_test " +
                            "WHERE group_id = 10 AND bucket = 2 ORDER BY id"));

            statement.executeUpdate(
                    "DELETE FROM composite_test WHERE id = 2");
            assertEquals(List.of("d"), queryPayloads(connection,
                    "SELECT payload FROM composite_test " +
                            "WHERE group_id = 10 AND bucket = 2 ORDER BY id"));

            statement.executeUpdate(
                    "UPDATE composite_test SET bucket = 9 WHERE id = 4");
            assertEquals(List.of(), queryPayloads(connection,
                    "SELECT payload FROM composite_test " +
                            "WHERE group_id = 10 AND bucket = 2 ORDER BY id"));
        } finally {
            database.close();
        }
    }

    @Test
    void uniqueIndexesRejectDuplicates() throws Exception {
        DBSystem database = createDatabase(true);
        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE unique_test ( id INTEGER, value INTEGER )");
            insertUniqueRow(connection, 1, 10);
            statement.executeUpdate(
                    "CREATE UNIQUE INDEX idx_unique_value ON unique_test(value)");

            assertThrows(SQLException.class,
                    () -> insertUniqueRow(connection, 2, 10));
        } finally {
            database.close();
        }
    }

    @Test
    void compositeIndexSupportsMultiColumnEquiJoin() throws Exception {
        DBSystem database = createDatabase(true);
        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE join_left_test (" +
                            " id INTEGER, group_id INTEGER, bucket INTEGER, " +
                            " payload VARCHAR(64) )");
            statement.executeUpdate(
                    "CREATE TABLE join_right_test (" +
                            " group_id INTEGER, bucket INTEGER )");
            statement.executeUpdate(
                    "CREATE INDEX idx_join_left_group_bucket " +
                            "ON join_left_test(group_id, bucket)");

            insertJoinLeftRow(connection, 1, 10, 1, "a");
            insertJoinLeftRow(connection, 2, 10, 2, "b");
            insertJoinLeftRow(connection, 3, 20, 1, "c");
            insertJoinRightRow(connection, 10, 2);
            insertJoinRightRow(connection, 20, 1);

            assertEquals(List.of("b", "c"), queryPayloads(connection,
                    "SELECT l.payload FROM join_left_test l, join_right_test r " +
                            "WHERE l.group_id = r.group_id " +
                            "AND l.bucket = r.bucket ORDER BY l.id"));
        } finally {
            database.close();
        }
    }

    @Test
    void dropIndexRemovesMetadataSoNameCanBeReused() throws Exception {
        DBSystem database = createDatabase(true);
        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE drop_index_test ( id INTEGER, value INTEGER )");
            statement.executeUpdate(
                    "CREATE INDEX idx_value ON drop_index_test(value)");
            statement.executeUpdate(
                    "DROP INDEX idx_value ON drop_index_test");
            statement.executeUpdate(
                    "CREATE INDEX idx_value ON drop_index_test(id)");
        } finally {
            database.close();
        }
    }

    @Test
    void indexDefinitionsSurviveRestart() throws Exception {
        DefaultDBConfig config = createConfig();
        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE restart_index_test (" +
                            " id INTEGER, group_id INTEGER, bucket INTEGER, " +
                            " payload VARCHAR(64) )");
            insertRestartRow(connection, 1, 7, 3, "before");
            statement.executeUpdate(
                    "CREATE INDEX idx_restart_group_bucket " +
                            "ON restart_index_test(group_id, bucket)");
        } finally {
            database.close();
        }

        database = DBController.getDefault().startDatabase(config);
        database.setDeleteOnClose(true);
        try (Connection connection = database.getConnection("test", "test")) {
            insertRestartRow(connection, 2, 7, 3, "after");
            assertEquals(List.of("after", "before"), queryPayloads(connection,
                    "SELECT payload FROM restart_index_test " +
                            "WHERE group_id = 7 AND bucket = 3 ORDER BY payload"));
        } finally {
            database.close();
        }
    }

    private DBSystem createDatabase(boolean deleteOnClose) {
        DBSystem database = DBController.getDefault()
                .createDatabase(createConfig(), "test", "test");
        database.setDeleteOnClose(deleteOnClose);
        return database;
    }

    private DefaultDBConfig createConfig() {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());
        return config;
    }

    private void insertRow(Connection connection, int id, int groupId,
                           int bucket, String payload) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO composite_test " +
                        "( id, group_id, bucket, payload ) VALUES ( ?, ?, ?, ? )")) {
            insert.setInt(1, id);
            insert.setInt(2, groupId);
            insert.setInt(3, bucket);
            insert.setString(4, payload);
            insert.executeUpdate();
        }
    }

    private void insertRestartRow(Connection connection, int id, int groupId,
                                  int bucket, String payload) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO restart_index_test " +
                        "( id, group_id, bucket, payload ) VALUES ( ?, ?, ?, ? )")) {
            insert.setInt(1, id);
            insert.setInt(2, groupId);
            insert.setInt(3, bucket);
            insert.setString(4, payload);
            insert.executeUpdate();
        }
    }

    private void insertUniqueRow(Connection connection, int id, int value)
            throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO unique_test ( id, value ) VALUES ( ?, ? )")) {
            insert.setInt(1, id);
            insert.setInt(2, value);
            insert.executeUpdate();
        }
    }

    private void insertJoinLeftRow(Connection connection, int id, int groupId,
                                   int bucket, String payload) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO join_left_test " +
                        "( id, group_id, bucket, payload ) VALUES ( ?, ?, ?, ? )")) {
            insert.setInt(1, id);
            insert.setInt(2, groupId);
            insert.setInt(3, bucket);
            insert.setString(4, payload);
            insert.executeUpdate();
        }
    }

    private void insertJoinRightRow(Connection connection, int groupId, int bucket)
            throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO join_right_test " +
                        "( group_id, bucket ) VALUES ( ?, ? )")) {
            insert.setInt(1, groupId);
            insert.setInt(2, bucket);
            insert.executeUpdate();
        }
    }

    private List<String> queryPayloads(Connection connection, String sql)
            throws Exception {
        List<String> payloads = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                payloads.add(result.getString(1));
            }
        }
        return payloads;
    }

}
