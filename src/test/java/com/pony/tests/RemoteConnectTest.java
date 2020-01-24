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

public class RemoteConnectTest {

    public static void main(String[] args) {

        try {
            Class.forName("com.pony.JDBCDriver");

            System.out.println("STARTED");

            Connection[] c = new Connection[50];

            for (int i = 0; i < 50; ++i) {
                c[i] =
                        DriverManager.getConnection("jdbc:pony://linux2", "test", "test");
            }

            System.out.println("FINISHED");

            Thread.sleep(10000);


        } catch (Throwable e) {
            e.printStackTrace();
        }

    }

}

