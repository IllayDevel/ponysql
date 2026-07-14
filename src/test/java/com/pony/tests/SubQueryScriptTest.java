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

/**
 * JUnit regression coverage for the legacy SubQuery.script scenarios.
 */
class SubQueryScriptTest {

    @TempDir
    Path tempDir;

    @Test
    void legacySubQueryScriptScenarios() throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            createTables(statement);
            populateTable(connection, "T1", List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
            populateTable(connection, "T2", List.of(3, 5, 7));
            populateTable(connection, "T3", List.of(2, 4, 6, 8, 10, 12));

            assertFirstColumn(statement,
                    "SELECT * FROM ( SELECT * FROM T1 ) ORDER BY c1",
                    List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
            assertFirstColumn(statement,
                    "SELECT c1 FROM ( SELECT c1 FROM T2 ) ORDER BY c1",
                    List.of(3, 5, 7));
            assertFirstColumn(statement,
                    "SELECT A.bb FROM ( SELECT c1 AS bb FROM T3 ) AS A ORDER BY bb",
                    List.of(2, 4, 6, 8, 10, 12));
            assertFirstColumn(statement,
                    "SELECT * FROM ( SELECT c1 FROM T1 WHERE c1 BETWEEN 3 AND 7 ) " +
                            "ORDER BY c1",
                    List.of(3, 4, 5, 6, 7));
            assertFirstColumn(statement,
                    "SELECT * FROM ( SELECT c1 FROM T1 WHERE c1 BETWEEN 3 AND 7 ) " +
                            "WHERE c1 BETWEEN 5 AND 6 OR c1 = 3 ORDER BY c1",
                    List.of(3, 5, 6));
            assertFirstColumn(statement,
                    "SELECT * FROM ( SELECT c1 FROM T1 WHERE c1 > 3 ), " +
                            "( SELECT (c1 / 2) c1div2 FROM T3 ) " +
                            "WHERE c1div2 = T1.c1 ORDER BY T1.c1",
                    List.of(4, 5, 6));

            assertFirstColumn(statement,
                    "SELECT * FROM T1 WHERE T1.c1 IN " +
                            "( SELECT T2.c1 FROM T2 ) ORDER BY T1.c1",
                    List.of(3, 5, 7));
            assertFirstColumn(statement,
                    "SELECT * FROM T1 WHERE T1.c1 NOT IN " +
                            "( SELECT T2.c1 FROM T2 ) ORDER BY T1.c1",
                    List.of(1, 2, 4, 6, 8, 9, 10));
            assertFirstColumn(statement,
                    "SELECT * FROM T1 WHERE T1.c1 > " +
                            "( SELECT AVG(T3.c1) FROM T3 ) ORDER BY T1.c1",
                    List.of(8, 9, 10));
            assertFirstColumn(statement,
                    "SELECT * FROM T1 WHERE T1.c1 > ANY " +
                            "( SELECT T3.c1 FROM T3 ) ORDER BY T1.c1",
                    List.of(3, 4, 5, 6, 7, 8, 9, 10));
            assertFirstColumn(statement,
                    "SELECT * FROM T1 WHERE T1.c1 > ALL ( SELECT 3 ) ORDER BY T1.c1",
                    List.of(4, 5, 6, 7, 8, 9, 10));
            assertFirstColumn(statement,
                    "SELECT * FROM T1 WHERE T1.c1 <> ALL " +
                            "( SELECT T3.c1 FROM T3 ) ORDER BY T1.c1",
                    List.of(1, 3, 5, 7, 9));
            assertFirstColumn(statement,
                    "SELECT * FROM T1 WHERE T1.c1 NOT IN " +
                            "( SELECT T3.c1 FROM T3 ) ORDER BY T1.c1",
                    List.of(1, 3, 5, 7, 9));

            assertFirstColumn(statement,
                    "SELECT * FROM T1, T2 WHERE T1.c1 = T2.c1 ORDER BY T1.c1",
                    List.of(3, 5, 7));
            assertFirstColumn(statement,
                    "SELECT * FROM T1, T2, T3 " +
                            "WHERE T1.c1 = T2.c1 AND T1.c1 > T3.c1 " +
                            "ORDER BY T1.c1",
                    List.of(3, 5, 5, 7, 7, 7));
            assertFirstColumn(statement,
                    "SELECT * FROM T1, T2, T3 " +
                            "WHERE T1.c1 = T2.c1 AND T1.c1 = T3.c1 " +
                            "ORDER BY T1.c1",
                    List.of());
            assertFirstColumn(statement,
                    "SELECT * FROM T1 LEFT OUTER JOIN T2 ON ( T1.c1 = T2.c1 ) " +
                            "ORDER BY T1.c1",
                    List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

            assertEquals(3, statement.executeUpdate(
                    "DELETE FROM T1 WHERE c1 IN ( SELECT c1 FROM T2 )"));
            assertEquals(7, statement.executeUpdate(
                    "UPDATE T1 SET c1 = c1 + 10000 " +
                            "WHERE c1 NOT IN ( SELECT c1 FROM T2 )"));
            assertFirstColumn(statement,
                    "SELECT c1 FROM T1 ORDER BY c1",
                    List.of(10001, 10002, 10004, 10006, 10008, 10009, 10010));
        } finally {
            database.close();
        }
    }

    private void createTables(Statement statement) throws Exception {
        for (String table : List.of("T1", "T2", "T3")) {
            statement.executeUpdate(
                    "CREATE TABLE " + table +
                            " ( c1 INTEGER, c2 VARCHAR(64) )");
            statement.executeUpdate(
                    "CREATE INDEX idx_" + table.toLowerCase() + "_c1 " +
                            "ON " + table + "(c1)");
        }
    }

    private void populateTable(Connection connection, String table,
                               List<Integer> values) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + table + " ( c1, c2 ) VALUES ( ?, ? )")) {
            for (Integer value : values) {
                insert.setInt(1, value);
                insert.setString(2, "Entry: " + value);
                assertEquals(1, insert.executeUpdate());
            }
        }
    }

    private void assertFirstColumn(Statement statement, String sql,
                                   List<Integer> expected) throws Exception {
        List<Integer> actual = new ArrayList<>();
        try (ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                actual.add(result.getInt(1));
            }
        }
        assertEquals(expected, actual);
    }

}
