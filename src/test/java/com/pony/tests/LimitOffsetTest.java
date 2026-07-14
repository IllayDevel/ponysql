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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LimitOffsetTest {

    @TempDir
    Path tempDir;

    @Test
    void limitOffsetIsAppliedAfterWhereAndOrderBy() throws Exception {
        DBSystem database = createDatabase();
        try (Connection connection = database.getConnection("test", "test")) {
            createRows(connection);

            assertEquals(
                    List.of(2, 3),
                    queryIds(connection,
                            "SELECT id FROM limit_test ORDER BY id LIMIT 2 OFFSET 1"));
            assertEquals(
                    List.of(3, 4),
                    queryIds(connection,
                            "SELECT id FROM limit_test WHERE id > 2 ORDER BY id LIMIT 2"));
        } finally {
            database.close();
        }
    }

    @Test
    void mysqlStyleLimitOffsetIsSupported() throws Exception {
        DBSystem database = createDatabase();
        try (Connection connection = database.getConnection("test", "test")) {
            createRows(connection);

            assertEquals(
                    List.of(3, 4),
                    queryIds(connection,
                            "SELECT id FROM limit_test ORDER BY id LIMIT 2, 2"));
        } finally {
            database.close();
        }
    }

    @Test
    void topNOrderByLimitHandlesMultipleColumnsAndDirections() throws Exception {
        DBSystem database = createDatabase();
        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE topn_test (" +
                            " id INTEGER, group_id INTEGER, bucket INTEGER )");

            insertTopNRow(connection, 1, 1, 2);
            insertTopNRow(connection, 2, 1, 1);
            insertTopNRow(connection, 3, 2, 2);
            insertTopNRow(connection, 4, 2, 1);
            insertTopNRow(connection, 5, 3, 1);

            assertEquals(
                    List.of(4, 3, 2),
                    queryIds(connection,
                            "SELECT id FROM topn_test " +
                                    "ORDER BY group_id DESC, bucket ASC " +
                                    "LIMIT 3 OFFSET 1"));
        } finally {
            database.close();
        }
    }

    @Test
    void limitZeroReturnsNoRows() throws Exception {
        DBSystem database = createDatabase();
        try (Connection connection = database.getConnection("test", "test")) {
            createRows(connection);

            assertEquals(
                    List.of(),
                    queryIds(connection,
                            "SELECT id FROM limit_test ORDER BY id LIMIT 0"));
        } finally {
            database.close();
        }
    }

    private DBSystem createDatabase() {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);
        return database;
    }

    private void createRows(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE limit_test ( id INTEGER, name VARCHAR(64) )");
        }

        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO limit_test ( id, name ) VALUES ( ?, ? )")) {
            for (int value : new int[]{5, 4, 3, 2, 1}) {
                insert.setInt(1, value);
                insert.setString(2, "row-" + value);
                insert.executeUpdate();
            }
        }
    }

    private void insertTopNRow(Connection connection, int id, int groupId,
                               int bucket) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO topn_test ( id, group_id, bucket ) " +
                        "VALUES ( ?, ?, ? )")) {
            insert.setInt(1, id);
            insert.setInt(2, groupId);
            insert.setInt(3, bucket);
            insert.executeUpdate();
        }
    }

    private List<Integer> queryIds(Connection connection, String sql) throws Exception {
        List<Integer> ids = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                ids.add(result.getInt(1));
            }
        }
        return ids;
    }
}
