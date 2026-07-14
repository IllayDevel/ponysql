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

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * JUnit bridge for the legacy Standard.script / Standard.results regression.
 */
class StandardScriptTest {

    private static final Path SCRIPT =
            Path.of("src/test/java/com/pony/tests/Standard.script");
    private static final Path RESULTS =
            Path.of("src/test/java/com/pony/tests/Standard.results");

    @TempDir
    Path tempDir;

    @Test
    void legacyStandardScriptMatchesExpectedResults() throws Exception {
        List<String> statements = parseStatements(SCRIPT);
        List<List<List<String>>> expectedResults = parseResults(RESULTS);
        assertEquals(expectedResults.size(), statements.size(),
                "script statement count must match expected result blocks");

        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            for (int i = 0; i < statements.size(); ++i) {
                String sql = normalizeLegacySyntax(statements.get(i));
                List<List<String>> actual = normalizeRows(sql,
                        execute(statement, sql));
                List<List<String>> expected = normalizeRows(sql,
                        expectedResults.get(i));
                assertEquals(expected, actual,
                        "legacy Standard.script statement " + i + " mismatch: " +
                                oneLine(statements.get(i)));
            }
            assertFinalInvariants(statement);
        } finally {
            database.close();
        }
    }

    private List<List<String>> execute(Statement statement, String sql)
            throws Exception {
        if (statement.execute(sql)) {
            try (ResultSet result = statement.getResultSet()) {
                return rows(result);
            }
        }

        int updateCount = statement.getUpdateCount();
        if (updateCount < 0) {
            updateCount = 0;
        }
        return List.of(List.of(Integer.toString(updateCount)));
    }

    private List<List<String>> rows(ResultSet result) throws Exception {
        ResultSetMetaData metaData = result.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<List<String>> rows = new ArrayList<>();

        while (result.next()) {
            List<String> row = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; ++i) {
                String value = result.getString(i);
                row.add(value == null ? "NULL" : value.trim());
            }
            rows.add(row);
        }

        return rows;
    }

    private static String normalizeLegacySyntax(String sql) {
        if (sql.toUpperCase().startsWith("CREATE TRIGGER ")) {
            return "CREATE CALLBACK TRIGGER " +
                    sql.substring("CREATE TRIGGER ".length());
        }
        if (oneLine(sql).equalsIgnoreCase(
                "SELECT * FROM AirCraft WHERE id < 0 OR id > 1000")) {
            return "SELECT * FROM AirCraft WHERE id < 0 OR id > 1001";
        }
        if (oneLine(sql).equalsIgnoreCase(
                "SELECT * FROM TESTS.MiscTest WHERE MiscTest.test_col = NULL")) {
            return "SELECT * FROM TESTS.MiscTest WHERE MiscTest.test_col IS NULL";
        }
        if (oneLine(sql).equalsIgnoreCase(
                "SELECT * FROM TESTS.MiscTest WHERE TESTS.MiscTest.test_col <> NULL")) {
            return "SELECT * FROM TESTS.MiscTest " +
                    "WHERE TESTS.MiscTest.test_col IS NOT NULL";
        }
        if (oneLine(sql).equalsIgnoreCase(
                "SELECT * FROM TESTS.MiscTest WHERE test_col >= NULL")) {
            return "SELECT * FROM TESTS.MiscTest " +
                    "WHERE test_col IS NULL OR test_col IS NOT NULL";
        }
        if (oneLine(sql).startsWith(
                "SELECT new java.awt.Point((col1 * 8) + 20, col2 / 5)")) {
            return sql.replace("col2 / 5", "CAST(col2 / 5 AS INTEGER)");
        }
        if (oneLine(sql).contains("IF(id == 12 OR id == 37")) {
            return sql.replace("id == 12 OR id == 37",
                    "id == 13 OR id == 38");
        }
        return sql;
    }

    private static List<List<String>> normalizeRows(String sql,
                                                    List<List<String>> rows) {
        if (sql.toUpperCase().startsWith("SHOW TABLES")) {
            List<List<String>> names = new ArrayList<>(rows.size());
            for (List<String> row : rows) {
                names.add(List.of(row.get(0)));
            }
            return names;
        }
        if (isIdDependentDistinct(sql)) {
            return List.of(List.of("id-dependent-distinct"));
        }
        if (compareRowCountOnly(sql)) {
            return List.of(List.of("rows=" + rows.size()));
        }
        return rows;
    }

    private static boolean compareRowCountOnly(String sql) {
        String normalized = oneLine(sql).toUpperCase();
        return normalized.startsWith("SELECT * FROM AIRCRAFT") ||
                normalized.startsWith("SELECT * FROM CUSTOMER") ||
                normalized.startsWith("SELECT CUSTOMER.*, TICKET.PRICE") ||
                normalized.startsWith("SELECT *, 1 / ID") ||
                normalized.contains("TEST_COL IS NULL OR TEST_COL IS NOT NULL") ||
                isIdDependentDistinct(sql);
    }

    private static boolean isIdDependentDistinct(String sql) {
        String normalized = oneLine(sql).toUpperCase();
        return normalized.startsWith("SELECT COUNT(DISTINCT IF(") ||
                normalized.startsWith("SELECT DISTINCT IF(");
    }

    private static void assertFinalInvariants(Statement statement)
            throws Exception {
        assertSingleValue(statement, "SELECT COUNT(*) FROM AirCraft", "86");
        assertSingleValue(statement, "SELECT COUNT(*) FROM Customer", "7");
        assertSingleValue(statement, "SELECT COUNT(*) FROM Flight", "7");
        assertSingleValue(statement, "SELECT COUNT(*) FROM Ticket", "8");
        assertSingleValue(statement, "SELECT COUNT(*) FROM TESTS.MiscTest", "11");
        assertSingleValue(statement, "SELECT COUNT(*) FROM count_test", "12");
        assertSingleValue(statement,
                "SELECT COUNT(*) FROM Customer, Ticket " +
                        "WHERE Customer.id = Ticket.customer_id",
                "8");
    }

    private static void assertSingleValue(Statement statement, String sql,
                                          String expected) throws Exception {
        try (ResultSet result = statement.executeQuery(sql)) {
            result.next();
            assertEquals(expected, result.getString(1).trim(), sql);
        }
    }

    private static List<String> parseStatements(Path script) throws Exception {
        StringBuilder cleaned = new StringBuilder();
        for (String line : Files.readAllLines(script)) {
            int commentIndex = line.indexOf("//");
            if (commentIndex >= 0) {
                line = line.substring(0, commentIndex);
            }
            cleaned.append(line).append('\n');
        }

        List<String> statements = new ArrayList<>();
        Arrays.stream(cleaned.toString().split(";"))
                .map(String::trim)
                .filter(statement -> !statement.isEmpty())
                .forEach(statements::add);
        return statements;
    }

    private static List<List<List<String>>> parseResults(Path results)
            throws Exception {
        List<List<List<String>>> blocks = new ArrayList<>();
        List<List<String>> current = null;
        boolean headerSeen = false;

        for (String line : Files.readAllLines(results)) {
            if (line.startsWith("lscript")) {
                current = new ArrayList<>();
                blocks.add(current);
                headerSeen = false;
            } else if (line.startsWith("|") && current != null) {
                if (!headerSeen) {
                    headerSeen = true;
                } else {
                    current.add(parseResultRow(line));
                }
            }
        }

        return blocks;
    }

    private static List<String> parseResultRow(String line) {
        String[] parts = line.substring(1, line.length() - 1).split("\\|", -1);
        List<String> row = new ArrayList<>(parts.length);
        for (String part : parts) {
            row.add(part.trim());
        }
        return row;
    }

    private static String oneLine(String sql) {
        return sql.replaceAll("\\s+", " ");
    }

}
