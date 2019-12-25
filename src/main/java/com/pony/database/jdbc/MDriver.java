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

package com.pony.database.jdbc;

import com.pony.database.control.DefaultDBConfig;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * JDBC implementation of the driver for the Pony database.
 * <p>
 * The url protocol is as follows:<p>
 * <pre>
 *  For connecting to a remote database server:
 *    jdbc:pony:[//hostname[:portnum]/][schema_name/]
 *
 *  eg.  jdbc:pony://db.pony.com:7009/
 *
 *  If hostname is not provided then it defaults to localhost.
 *  If portnum is not provided it defaults to 9157.
 *  If schema_name is not provided it defaults to APP.
 *
 *  To start up a database in the local file system the protocol is:
 *    jdbc:pony:local://databaseconfiguration/[schema_name/]
 *
 *  eg.  jdbc:pony:local://D:/dbdata/db.conf
 *
 *  If schema_name is not provided it defaults to APP.
 *
 *  To create a database in the local file system then you need to supply a
 *  'create=true' assignment in the URL encoding.
 *
 *  eg.  jdbc:pony:local://D:/dbdata/db.conf?create=true
 * </pre>
 * <p>
 * A local database runs within the JVM of this JDBC driver.  To boot a
 * local database, you must include the full database .jar release with
 * your application distribution.
 * <p>
 * For connecting to a remote database using the remote URL string, only the
 * JDBC driver need be included in the classpath.
 * <p>
 * NOTE: This needs to be a light-weight object, because a developer could
 *   generate multiple instances of this class.  Making an instance of
 *   'com.pony.JDBCDriver' will create at least two instances of this object.
 *
 * @author Tobias Downer
 */

public class MDriver implements Driver {

    // The major and minor version numbers of the driver.  This only changes
    // when the JDBC communcation protocol changes.
    static final int DRIVER_MAJOR_VERSION = 1;
    static final int DRIVER_MINOR_VERSION = 0;

    // The name of the driver.
    static final String DRIVER_NAME = "Pony JDBC Driver";
    // The version of the driver as a string.
    static final String DRIVER_VERSION =
            "" + DRIVER_MAJOR_VERSION + "." + DRIVER_MINOR_VERSION;


    // The protocol URL header string that signifies a Pony JDBC connection.
    private static final String pony_protocol_url = "jdbc:pony:";


    /**
     * Set to true when this driver is registered.
     */
    private static boolean registered = false;


    // ----- Static methods -----

    /**
     * Static method that registers this driver with the JDBC driver manager.
     */
    public synchronized static void register() {
        if (registered == false) {
            try {
                java.sql.DriverManager.registerDriver(new MDriver());
                registered = true;
            } catch (SQLException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    // ----- MDriver -----

    /**
     * The timeout for a query in seconds.
     */
    static int QUERY_TIMEOUT = Integer.MAX_VALUE;

    /**
     * The mapping of the database configuration URL string to the LocalBootable
     * object that manages the connection.  This mapping is only used if the
     * driver makes local connections (eg. 'jdbc:pony:local://').
     */
    private final Map local_session_map;

    /**
     * Constructor is public so that instances of the JDBC driver can be
     * created by developers.
     */
    public MDriver() {
        local_session_map = new HashMap();
    }

    /**
     * Given a URL encoded arguments string, this will extract the var=value
     * pairs and put them in the given Properties object.  For example,
     * the string 'create=true&user=usr&password=passwd' will extract the three
     * values and put them in the Properties object.
     */
    private static void parseEncodedVariables(String url_vars, Properties info) {

        // Parse the url variables.
        StringTokenizer tok = new StringTokenizer(url_vars, "&");
        while (tok.hasMoreTokens()) {
            String token = tok.nextToken().trim();
            int split_point = token.indexOf("=");
            if (split_point > 0) {
                String key = token.substring(0, split_point).toLowerCase();
                String value = token.substring(split_point + 1);
                // Put the key/value pair in the 'info' object.
                info.put(key, value);
            } else {
                System.err.println("Ignoring url variable: '" + token + "'");
            }
        } // while (tok.hasMoreTokens())

    }

    /**
     * Creates a new LocalBootable object that is used to manage the connections
     * to a database running locally.  This uses reflection to create a new
     * com.pony.database.jdbcserver.DefaultLocalBootable object.  We use
     * reflection here because we don't want to make a source level dependency
     * link to the class.  Throws an SQLException if the class was not found.
     */
    private static LocalBootable createDefaultLocalBootable()
            throws SQLException {
        try {
            Class c = Class.forName(
                    "com.pony.database.jdbcserver.DefaultLocalBootable");
            return (LocalBootable) c.newInstance();
        } catch (Throwable e) {
            // A lot of people ask us about this error so the message is verbose.
            throw new SQLException(
                    "I was unable to find the class that manages local database " +
                            "connections.  This means you may not have included the correct " +
                            "library in your classpath.  Make sure that either ponydb.jar " +
                            "is in your classpath or your classpath references the complete " +
                            "Pony SQL database class hierarchy.");
        }
    }

    /**
     * Makes a connection to a local database.  If a local database connection
     * has not been made then it is created here.
     * <p>
     * Returns a list of two elements, (DatabaseInterface) db_interface and
     * (String) database_name.
     */
    private synchronized Object[] connectToLocal(String url, String address_part,
                                                 Properties info) throws SQLException {

        // If the LocalBootable object hasn't been created yet, do so now via
        // reflection.
        String schema_name = "APP";
        DatabaseInterface db_interface;

        // Look for the name upto the URL encoded variables
        int url_start = address_part.indexOf("?");
        if (url_start == -1) {
            url_start = address_part.length();
        }

        // The path to the configuration
        String config_path = address_part.substring(8, url_start);

        // If no config_path, then assume it is ./db.conf
        if (config_path.length() == 0) {
            config_path = "./db.conf";
        }

        // Substitute win32 '\' to unix style '/'
        config_path = config_path.replace('\\', '/');

        // Is the config path encoded as a URL?
        if (config_path.startsWith("jar:") ||
                config_path.startsWith("file:/") ||
                config_path.startsWith("ftp:/") ||
                config_path.startsWith("http:/") ||
                config_path.startsWith("https:/")) {
            // Don't do anything - looks like a URL already.
        } else {

            // We don't care about anything after the ".conf/"
            String abs_path;
            String post_abs_path;
            int schem_del = config_path.indexOf(".conf/");
            if (schem_del == -1) {
                abs_path = config_path;
                post_abs_path = "";
            } else {
                abs_path = config_path.substring(0, schem_del + 5);
                post_abs_path = config_path.substring(schem_del + 5);
            }

            // If the config_path contains the string "!/" then assume this is a jar
            // file configuration reference.  For example,
            //  'C:/my_db/my_jar.jar!/configs/db.conf'

            // If the config path is not encoded as a URL, add a 'file:/' preffix
            // to the path to make it a URL.  For example 'C:/my_config.conf" becomes
            // 'file:/C:/my_config.conf', 'C:/my_libs/my_jar.jar!/configs/db.conf'
            // becomes 'jar:file:/C:/my_libs/my_jar.jar!/configs/db.conf'

            int jar_delim_i = abs_path.indexOf("!/");
            String path_part = abs_path;
            String rest_part = "";
            String pre = "file:/";
            if (jar_delim_i != -1) {
                path_part = abs_path.substring(0, jar_delim_i);
                rest_part = abs_path.substring(jar_delim_i);
                pre = "jar:file:/";
            }

            // Does the configuration file exist?  Or does the resource that contains
            // the configuration exist?
            // We try the file with a preceeding '/' and without.
            File f = new File(path_part);
            if (!f.exists() && !path_part.startsWith("/")) {
                f = new File("/" + path_part);
                if (!f.exists()) {
                    throw new SQLException("Unable to find file: " + path_part);
                }
            }
            // Construct the new qualified configuration path.
            config_path = pre + f.getAbsolutePath() + rest_part + post_abs_path;
            // Substitute win32 '\' to unix style '/'
            // We do this (again) because on win32 'f.getAbsolutePath()' returns win32
            // style deliminators.
            config_path = config_path.replace('\\', '/');
        }

        // Look for the string '.conf/' in the config_path which is used to
        // determine the initial schema name.  For example, the connection URL,
        // 'jdbc:pony:local:///my_db/db.conf/TOBY' will start the database in the
        // TOBY schema of the database denoted by the configuration path
        // '/my_db/db.conf'
        int schema_del_i = config_path.toLowerCase().indexOf(".conf/");
        if (schema_del_i > 0 &&
                schema_del_i + 6 < config_path.length()) {
            schema_name = config_path.substring(schema_del_i + 6);
            config_path = config_path.substring(0, schema_del_i + 5);
        }

        // The url variables part
        String url_vars = "";
        if (url_start < address_part.length()) {
            url_vars = address_part.substring(url_start + 1).trim();
        }

        // Is there already a local connection to this database?
        String session_key = config_path.toLowerCase();
        LocalBootable local_bootable =
                (LocalBootable) local_session_map.get(session_key);
        // No so create one and put it in the connection mapping
        if (local_bootable == null) {
            local_bootable = createDefaultLocalBootable();
            local_session_map.put(session_key, local_bootable);
        }

        // Is the connection booted already?
        if (local_bootable.isBooted()) {
            // Yes, so simply login.
            db_interface = local_bootable.connectToJVM();
        } else {
            // Otherwise we need to boot the local database.

            // This will be the configuration input file
            InputStream config_in;
            if (!config_path.startsWith("file:/")) {
                // Make the config_path into a URL and open an input stream to it.
                URL config_url;
                try {
                    config_url = new URL(config_path);
                } catch (MalformedURLException e) {
                    throw new SQLException("Malformed URL: " + config_path);
                }

                try {
                    // Try and open an input stream to the given configuration.
                    config_in = config_url.openConnection().getInputStream();
                } catch (IOException e) {
                    throw new SQLException("Unable to open configuration file.  " +
                            "I tried looking at '" + config_url.toString() + "'");
                }
            } else {
                try {
                    // Try and open an input stream to the given configuration.
                    config_in = new FileInputStream(new File(config_path.substring(6)));
                } catch (IOException e) {
                    throw new SQLException("Unable to open configuration file: " +
                            config_path);
                }

            }

            // Work out the root path (the place in the local file system where the
            // configuration file is).
            File root_path;
            // If the URL is a file, we can work out what the root path is.
            if (config_path.startsWith("jar:file:/") ||
                    config_path.startsWith("file:/")) {

                int start_i = config_path.indexOf(":/");

                // If the config_path is pointing inside a jar file, this denotes the
                // end of the file part.
                int file_end_i = config_path.indexOf("!");
                String config_file_part;
                if (file_end_i == -1) {
                    config_file_part = config_path.substring(start_i + 2);
                } else {
                    config_file_part = config_path.substring(start_i + 2, file_end_i);
                }

                File absolute_config_file = new File(
                        new File(config_file_part).getAbsolutePath());
                root_path = new File(absolute_config_file.getParent());
            } else {
                // This means the configuration file isn't sitting in the local file
                // system, so we assume root is the current directory.
                root_path = new File(".");
            }

            // Get the configuration bundle that was set as the path,
            DefaultDBConfig config = new DefaultDBConfig(root_path);
            try {
                config.loadFromStream(config_in);
                config_in.close();
            } catch (IOException e) {
                throw new SQLException("Error reading configuration file: " +
                        config_path + " Reason: " + e.getMessage());
            }

            // Parse the url variables
            parseEncodedVariables(url_vars, info);

            boolean create_db = false;
            boolean create_db_if_not_exist = false;
            create_db = info.getProperty("create", "").equals("true");
            create_db_if_not_exist =
                    info.getProperty("boot_or_create", "").equals("true") ||
                            info.getProperty("create_or_boot", "").equals("true");

            // Include any properties from the 'info' object
            Enumeration prop_keys = info.keys();
            while (prop_keys.hasMoreElements()) {
                String key = prop_keys.nextElement().toString();
                if (!key.equals("user") && !key.equals("password")) {
                    config.setValue(key, (String) info.get(key));
                }
            }

            // Check if the database exists
            boolean database_exists = local_bootable.checkExists(config);

            // If database doesn't exist and we've been told to create it if it
            // doesn't exist, then set the 'create_db' flag.
            if (create_db_if_not_exist && !database_exists) {
                create_db = true;
            }

            // Error conditions;
            // If we are creating but the database already exists.
            if (create_db && database_exists) {
                throw new SQLException(
                        "Can not create database because a database already exists.");
            }
            // If we are booting but the database doesn't exist.
            if (!create_db && !database_exists) {
                throw new SQLException(
                        "Can not find a database to start.  Either the database needs to " +
                                "be created or the 'database_path' property of the configuration " +
                                "must be set to the location of the data files.");
            }

            // Are we creating a new database?
            if (create_db) {
                String username = info.getProperty("user", "");
                String password = info.getProperty("password", "");

                db_interface = local_bootable.create(username, password, config);
            }
            // Otherwise we must be logging onto a database,
            else {
                db_interface = local_bootable.boot(config);
            }
        }

        // Make up the return parameters.
        Object[] ret = new Object[2];
        ret[0] = db_interface;
        ret[1] = schema_name;

        return ret;

    }


    // ---------- Implemented from Driver ----------

    public Connection connect(String url, Properties info) throws SQLException {
        // We looking for url starting with this protocol
        if (!acceptsURL(url)) {
            // If the protocol not valid then return null as in the spec.
            return null;
        }

        DatabaseInterface db_interface;
        String default_schema = "APP";

        int row_cache_size;
        int max_row_cache_size;

        String address_part = url.substring(url.indexOf(pony_protocol_url) +
                pony_protocol_url.length());
        // If we are to connect this JDBC to a single user database running
        // within this JVM.
        if (address_part.startsWith("local://")) {

            // Returns a list of two Objects, db_interface and database_name.
            Object[] ret_list = connectToLocal(url, address_part, info);
            db_interface = (DatabaseInterface) ret_list[0];
            default_schema = (String) ret_list[1];

            // Internal row cache setting are set small.
            row_cache_size = 43;
            max_row_cache_size = 4092000;

        } else {
            int port = 9157;
            String host = "127.0.0.1";

            // Otherwise we must be connecting remotely.
            if (address_part.startsWith("//")) {

                String args_string = "";
                int arg_part = address_part.indexOf('?', 2);
                if (arg_part != -1) {
                    args_string = address_part.substring(arg_part + 1);
                    address_part = address_part.substring(0, arg_part);
                }

//        System.out.println("ADDRESS_PART: " + address_part);

                int end_address = address_part.indexOf("/", 2);
                if (end_address == -1) {
                    end_address = address_part.length();
                }
                String remote_address = address_part.substring(2, end_address);
                int delim = remote_address.indexOf(':');
                if (delim == -1) {
                    delim = remote_address.length();
                }
                host = remote_address.substring(0, delim);
                if (delim < remote_address.length() - 1) {
                    port = Integer.parseInt(remote_address.substring(delim + 1));
                }

//        System.out.println("REMOTE_ADDRESS: '" + remote_address + "'");

                // Schema name?
                String schema_part = "";
                if (end_address < address_part.length()) {
                    schema_part = address_part.substring(end_address + 1);
                }
                String schema_string = schema_part;
                int schema_end = schema_part.indexOf('/');
                if (schema_end != -1) {
                    schema_string = schema_part.substring(0, schema_end);
                } else {
                    schema_end = schema_part.indexOf('?');
                    if (schema_end != -1) {
                        schema_string = schema_part.substring(0, schema_end);
                    }
                }

//        System.out.println("SCHEMA_STRING: '" + schema_string + "'");

                // Argument part?
                if (!args_string.equals("")) {
//          System.out.println("ARGS: '" + args_string + "'");
                    parseEncodedVariables(args_string, info);
                }

                // Is there a schema or should we default?
                if (schema_string.length() > 0) {
                    default_schema = schema_string;
                }

            } else {
                if (address_part.trim().length() > 0) {
                    throw new SQLException("Malformed URL: " + address_part);
                }
            }

//      database_name = address_part;
//      if (database_name == null || database_name.trim().equals("")) {
//        database_name = "DefaultDatabase";
//      }

            // BUG WORKAROUND:
            // There appears to be a bug in the socket code of some VM
            // implementations.  With the IBM Linux JDK, if a socket is opened while
            // another is closed while blocking on a read, the socket that was just
            // opened breaks.  This was causing the login code to block indefinitely
            // and the connection thread causing a null pointer exception.
            // The workaround is to put a short pause before the socket connection
            // is established.
            try {
                Thread.sleep(85);
            } catch (InterruptedException e) { /* ignore */ }

            // Make the connection
            TCPStreamDatabaseInterface tcp_db_interface =
                    new TCPStreamDatabaseInterface(host, port);
            // Attempt to open a socket to the database.
            tcp_db_interface.connectToDatabase();

            db_interface = tcp_db_interface;

            // For remote connection, row cache uses more memory.
            row_cache_size = 4111;
            max_row_cache_size = 8192000;

        }

//    System.out.println("DEFAULT SCHEMA TO CONNECT TO: " + default_schema);

        // Create the connection object on the given database,
        MConnection connection = new MConnection(url, db_interface,
                row_cache_size, max_row_cache_size);
        // Try and login (throws an SQLException if fails).
        connection.login(info, default_schema);

        return connection;
    }

    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(pony_protocol_url) ||
                url.startsWith(":" + pony_protocol_url);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        // Is this for asking for usernames and passwords if they are
        // required but not provided?

        // Return nothing for now, assume required info has been provided.
        return new DriverPropertyInfo[0];
    }

    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    public boolean jdbcCompliant() {
        // Certified compliant? - perhaps one day...
        return false;
    }

//#IFDEF(JDBC5.0)

    // -------------------------- JDK 1.7 -----------------------------------

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

//#ENDIF

}
