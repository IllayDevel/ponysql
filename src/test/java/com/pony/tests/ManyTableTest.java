/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pony.tests;

import java.sql.*;

/**
 *
 *
 * @author Tobias Downer
 */

public class ManyTableTest {

    public static void main(String[] args) {

        try {

            Class.forName("com.pony.JDBCDriver");

            Connection c = DriverManager.getConnection(
                    "jdbc:pony:local://db.conf", "test", "test");
            Statement stmt = c.createStatement();

            for (int i = 0; i < 1000; ++i) {
                stmt.executeQuery("CREATE TABLE Table" + i + " ( c1 int, c2 varchar )");
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }

    }

}

