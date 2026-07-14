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

import com.pony.database.control.DBConfig;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Regression coverage for the old manual ControlAPITest2 main program.
 */
class ControlApiRestartTest {

    @TempDir
    Path tempDir;

    @Test
    void controlApiRestartsDatabaseAndKeepsCommittedRows()
            throws Exception {
        DBController controller = DBController.getDefault();
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = controller.createDatabase(config, "test", "test");
        try {
            populateDatabase(database);
        } finally {
            database.close();
        }

        for (int i = 0; i < 10; ++i) {
            assertRestartedDatabase(config);
        }

        database = controller.startDatabase(config);
        database.setDeleteOnClose(true);
        database.close();
    }

    private void populateDatabase(DBSystem database) throws Exception {
        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertNotNull(metaData.getDatabaseProductName());

            connection.setAutoCommit(false);
            statement.executeQuery(
                    "CREATE TABLE test ( a INTEGER, b TEXT )");

            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO test ( a, b ) VALUES ( ?, ? )")) {
                for (int i = 1; i <= 100; ++i) {
                    insert.setInt(1, i);
                    insert.setString(2, "record:" + i);
                    assertEquals(1, insert.executeUpdate());
                }
            }

            connection.commit();
            assertSelectedRows(connection);
        }
    }

    private void assertRestartedDatabase(DBConfig config) throws Exception {
        DBSystem session = DBController.getDefault().startDatabase(config);
        try (Connection connection = session.getConnection("test", "test")) {
            assertSelectedRows(connection);
        } finally {
            session.close();
        }
    }

    private void assertSelectedRows(Connection connection) throws Exception {
        int rowCount = 0;
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT a, b FROM test WHERE a <= 10 OR a >= 60")) {
            while (result.next()) {
                int value = result.getInt(1);
                assertEquals("record:" + value, result.getString(2));
                ++rowCount;
            }
        }
        assertEquals(51, rowCount);
    }

}
