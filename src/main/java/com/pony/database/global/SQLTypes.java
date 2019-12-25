/*
 * Pony SQL Database ( http://www.ponysql.ru/ )
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

package com.pony.database.global;

/**
 * A JDBC independant type definition list.  This allows the specification of
 * all JDBC 1.0 and 2.0 types without requiring the JDBC 2.0
 * 'java.sql.Types' interface.
 * <p>
 * The values are compatible with the JDBC 1.0 and 2.0 spec.
 *
 * @author Tobias Downer
 */

public interface SQLTypes {

    int BIT = -7;

    int TINYINT = -6;

    int SMALLINT = 5;

    int INTEGER = 4;

    int BIGINT = -5;

    int FLOAT = 6;

    int REAL = 7;

    int DOUBLE = 8;

    int NUMERIC = 2;

    int DECIMAL = 3;

    int CHAR = 1;

    int VARCHAR = 12;

    int LONGVARCHAR = -1;

    int DATE = 91;

    int TIME = 92;

    int TIMESTAMP = 93;

    int BINARY = -2;

    int VARBINARY = -3;

    int LONGVARBINARY = -4;

    int NULL = 0;

    int OTHER = 1111;

    int JAVA_OBJECT = 2000;

    int DISTINCT = 2001;

    int STRUCT = 2002;

    int ARRAY = 2003;

    int BLOB = 2004;

    int CLOB = 2005;

    int REF = 2006;

    int BOOLEAN = 16;

}
