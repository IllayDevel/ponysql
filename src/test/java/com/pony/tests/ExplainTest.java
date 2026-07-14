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
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExplainTest {

    @TempDir
    Path tempDir;

    @Test
    void explainSelectReturnsQueryPlanDebugString() throws Exception {
        DefaultDBConfig config = new DefaultDBConfig();
        config.setDatabasePath(tempDir.resolve("data").toString());
        config.setLogPath(tempDir.resolve("log").toString());

        DBSystem database = DBController.getDefault()
                .createDatabase(config, "test", "test");
        database.setDeleteOnClose(true);

        try (Connection connection = database.getConnection("test", "test");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE explain_test ( id INTEGER, name VARCHAR(64) )");
            statement.executeUpdate(
                    "INSERT INTO explain_test ( id, name ) VALUES ( 1, 'pony' )");

            try (ResultSet result = statement.executeQuery(
                    "EXPLAIN SELECT name FROM explain_test " +
                            "WHERE id = 1 ORDER BY name LIMIT 1")) {
                assertTrue(result.next());
                String plan = result.getString(1);
                assertTrue(plan.contains("FETCH: APP.explain_test"));
                assertTrue(plan.contains("SIMPLE:") || plan.contains("RANGE:"));
                assertTrue(plan.contains("LIMIT/OFFSET: offset=0, limit=1"));
                assertFalse(result.next());
            }
        } finally {
            database.close();
        }
    }

}
