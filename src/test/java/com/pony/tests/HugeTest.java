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
 * Regression coverage for the old manual HugeTest main program.
 */
class HugeTest {

    private static final int INT_FIELDS = 139;
    private static final int DEC_FIELDS = 79;
    private static final int STRING_FIELDS = 10;
    private static final int ROW_COUNT = 16;

    @TempDir
    Path tempDir;

    @Test
    void createsInsertsAndScansWideTable() throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.executeUpdate(createTableSql());
            try (PreparedStatement insert =
                         connection.prepareStatement(insertSql())) {
                for (int row = 1; row <= ROW_COUNT; ++row) {
                    bindRow(insert, row);
                    assertEquals(1, insert.executeUpdate());
                }
            }
            connection.commit();

            int scanned = 0;
            try (ResultSet result = statement.executeQuery(
                    "SELECT * FROM HUGETABLE ORDER BY INTFIELD1")) {
                assertEquals(
                        INT_FIELDS + DEC_FIELDS + STRING_FIELDS,
                        result.getMetaData().getColumnCount());
                while (result.next()) {
                    ++scanned;
                    assertEquals(scanned * 1000 + 1,
                            result.getInt("HUGETABLE.INTFIELD1"));
                    assertEquals(scanned * 1000 + INT_FIELDS,
                            result.getInt("HUGETABLE.INTFIELD" + INT_FIELDS));
                    assertEquals(scanned * 100 + DEC_FIELDS,
                            result.getInt("HUGETABLE.DECFIELD" + DEC_FIELDS));
                    assertEquals("row-" + scanned + "-string-" + STRING_FIELDS,
                            result.getString("HUGETABLE.STRINGFIELD" + STRING_FIELDS));
                }
            }
            assertEquals(ROW_COUNT, scanned);

            try (ResultSet status = statement.executeQuery("SHOW STATUS")) {
                assertTrue(status.next());
                assertFalse(status.isAfterLast());
            }
        } finally {
            database.close();
        }
    }

    private String createTableSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE HUGETABLE(");
        for (int i = 1; i <= INT_FIELDS; ++i) {
            appendColumn(sql, "INTFIELD" + i + " INTEGER INDEX_NONE");
        }
        for (int i = 1; i <= DEC_FIELDS; ++i) {
            appendColumn(sql, "DECFIELD" + i + " DECIMAL(9,2) INDEX_NONE");
        }
        for (int i = 1; i <= STRING_FIELDS; ++i) {
            appendColumn(sql, "STRINGFIELD" + i + " VARCHAR(40) INDEX_NONE");
        }
        sql.append(")");
        return sql.toString();
    }

    private void appendColumn(StringBuilder sql, String columnDef) {
        if (sql.charAt(sql.length() - 1) != '(') {
            sql.append(", ");
        }
        sql.append(columnDef);
    }

    private String insertSql() {
        int columnCount = INT_FIELDS + DEC_FIELDS + STRING_FIELDS;
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO HUGETABLE VALUES (");
        for (int i = 0; i < columnCount; ++i) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
        sql.append(")");
        return sql.toString();
    }

    private void bindRow(PreparedStatement insert, int row) throws Exception {
        int parameter = 1;
        for (int i = 1; i <= INT_FIELDS; ++i) {
            insert.setInt(parameter++, row * 1000 + i);
        }
        for (int i = 1; i <= DEC_FIELDS; ++i) {
            insert.setInt(parameter++, row * 100 + i);
        }
        for (int i = 1; i <= STRING_FIELDS; ++i) {
            insert.setString(parameter++, "row-" + row + "-string-" + i);
        }
    }

}
