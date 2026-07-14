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
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression coverage for the old manual InsertDeleteTest main program.
 */
class InsertDeleteTest {

    private static final int WORKER_COUNT = 3;
    private static final int ROWS_PER_WORKER = 180;

    @TempDir
    Path tempDir;

    @Test
    void insertDeleteKeepsIndexesAndOrderingCurrent()
            throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE Test1 ( " +
                            " cola VARCHAR(60), " +
                            " colb VARCHAR(60), " +
                            " colc INTEGER, " +
                            " cold NUMERIC, " +
                            " cole BOOLEAN )");
            statement.executeUpdate("CREATE INDEX idx_test1_colc ON Test1(colc)");
            statement.executeUpdate("CREATE INDEX idx_test1_cold ON Test1(cold)");

            List<WorkerResult> workerResults = new ArrayList<>();
            for (int worker = 0; worker < WORKER_COUNT; ++worker) {
                int base = worker * 10000;
                workerResults.add(runWorker(database, base));
            }

            List<Integer> expectedColc = new ArrayList<>();
            List<Double> expectedCold = new ArrayList<>();
            for (WorkerResult result : workerResults) {
                expectedColc.addAll(result.colcValues);
                expectedCold.addAll(result.coldValues);
            }
            Collections.sort(expectedColc);
            Collections.sort(expectedCold);

            assertEquals(expectedColc, queryInts(statement,
                    "SELECT colc FROM Test1 ORDER BY colc"));
            assertEquals(expectedCold, queryDoubles(statement,
                    "SELECT cold FROM Test1 ORDER BY cold"));
        } finally {
            database.close();
        }
    }

    private WorkerResult runWorker(DBSystem database, int base) throws Exception {
        try (Connection connection = database.getConnection("test", "test")) {
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO Test1 ( cola, colb, colc, cold, cole ) " +
                            "VALUES ( ?, ?, ?, ?, ? )");
                 PreparedStatement delete = connection.prepareStatement(
                         "DELETE FROM Test1 WHERE colc = ?");
                 PreparedStatement select = connection.prepareStatement(
                         "SELECT cold FROM Test1 WHERE colc = ?")) {

                List<Integer> colcValues = new ArrayList<>();
                List<Double> coldValues = new ArrayList<>();

                for (int i = 0; i < ROWS_PER_WORKER; ++i) {
                    int colc = base + i;
                    double cold = (base / 1000) + (i % 17);
                    insert.setString(1, "Yippy");
                    insert.setString(2, "This is kind of a long string.");
                    insert.setInt(3, colc);
                    insert.setDouble(4, cold);
                    insert.setBoolean(5, (i % 2) == 0);
                    assertEquals(1, insert.executeUpdate());
                    colcValues.add(colc);
                    coldValues.add(cold);

                    if (i >= 30 && (i % 4) == 0) {
                        int deleteColc = base + i - 30;
                        int index = colcValues.indexOf(deleteColc);
                        if (index >= 0) {
                            assertStoredValue(select, deleteColc,
                                    coldValues.get(index));
                            delete.setInt(1, deleteColc);
                            assertEquals(1, delete.executeUpdate());
                            colcValues.remove(index);
                            coldValues.remove(index);
                        }
                    }
                }
                return new WorkerResult(colcValues, coldValues);
            }
        }
    }

    private void assertStoredValue(PreparedStatement select, int colc,
                                   double expectedCold) throws Exception {
        select.setInt(1, colc);
        try (ResultSet result = select.executeQuery()) {
            result.next();
            assertEquals(expectedCold, result.getDouble(1), 0.0);
        }
    }

    private List<Integer> queryInts(Statement statement, String sql)
            throws Exception {
        List<Integer> values = new ArrayList<>();
        try (ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                values.add(result.getInt(1));
            }
        }
        return values;
    }

    private List<Double> queryDoubles(Statement statement, String sql)
            throws Exception {
        List<Double> values = new ArrayList<>();
        try (ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                values.add(result.getDouble(1));
            }
        }
        return values;
    }

    private static class WorkerResult {
        private final List<Integer> colcValues;
        private final List<Double> coldValues;

        WorkerResult(List<Integer> colcValues, List<Double> coldValues) {
            this.colcValues = colcValues;
            this.coldValues = coldValues;
        }
    }

}
