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

package com.pony.database;

import com.pony.store.LoggingBufferManager;
import com.pony.util.Stats;
import com.pony.util.StringUtil;
import com.pony.util.LogWriter;
import com.pony.debug.*;
import com.pony.database.control.DBConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

/**
 * A class that provides information and global functions for the transaction
 * layer in the engine.  Shared information includes configuration details,
 * logging, etc.
 *
 * @author Tobias Downer
 */

public class TransactionSystem {

    /**
     * The stats object that keeps track of database statistics.
     */
    private final Stats stats = new Stats();

    /**
     * A logger to output any debugging messages.
     * NOTE: This MUST be final, because other objects may retain a reference
     *   to the object.  If it is not final, then different objects will be
     *   logging to different places if this reference is changed.
     */
    private final DefaultDebugLogger logger;

    /**
     * The ResourceBundle that contains properties of the entire database
     * system.
     */
    private DBConfig config = null;

    /**
     * The path in the file system for the database files.  Note that this will
     * be null if the database does not exist in a local file system.  For this
     * reason it's best not to write code that relies on the use of this value.
     */
    private File db_path;

    /**
     * Set to true if lookup comparison lists are enabled.
     */
    private boolean lookup_comparison_list_enabled = false;

    /**
     * Set to true if the database is in read only mode.  This is set from the
     * configuration file.
     */
    private boolean read_only_access = false;

    /**
     * Set to true if the parser should ignore case when searching for a schema,
     * table or column using an identifier.
     */
    private boolean ignore_case_for_identifiers = false;

    /**
     * Transaction option, if this is true then a transaction error is generated
     * during commit if a transaction selects data from a table that has
     * committed changes to it during commit time.
     * <p>
     * True by default.
     */
    private boolean transaction_error_on_dirty_select = true;

    /**
     * The DataCellCache that is a shared resource between on database's.
     */
    private DataCellCache data_cell_cache = null;

    /**
     * The list of FunctionFactory objects that handle different functions from
     * SQL.
     */
    private ArrayList function_factory_list;

    /**
     * The FunctionLookup object that can resolve a FunctionDef object to a
     * Function object.
     */
    private DSFunctionLookup function_lookup;

    /**
     * The regular expression library bridge for the library we are configured
     * to use.
     */
    private RegexLibrary regex_library;

    /**
     * The log directory.
     */
    private File log_directory;

    /**
     * A LoggingBufferManager object used to manage pages of ScatteringFileStore
     * objects in the file system.  We can configure the maximum pages and page
     * size via this object, so we have control over how much memory from the
     * heap is used for buffering.
     */
    private LoggingBufferManager buffer_manager;

    /**
     * The underlying StoreSystem implementation that encapsulates the behaviour
     * for storing data persistantly.
     */
    private StoreSystem store_system;

    // ---------- Low level row listeners ----------


    /**
     * Constructor.
     */
    public TransactionSystem() {
        // Setup generate properties from the JVM.
        logger = new DefaultDebugLogger();
        Properties p = System.getProperties();
        stats.set(0, "Runtime.java.version: " + p.getProperty("java.version"));
        stats.set(0, "Runtime.java.vendor: " + p.getProperty("java.vendor"));
        stats.set(0, "Runtime.java.vm.name: " + p.getProperty("java.vm.name"));
        stats.set(0, "Runtime.os.name: " + p.getProperty("os.name"));
        stats.set(0, "Runtime.os.arch: " + p.getProperty("os.arch"));
        stats.set(0, "Runtime.os.version: " + p.getProperty("os.version"));
        ArrayList table_listeners = new ArrayList();
    }

    /**
     * Parses a file string to an absolute position in the file system.  We must
     * provide the path to the root directory (eg. the directory where the
     * config bundle is located).
     */
    private static File parseFileString(File root_path, String root_info,
                                        String path_string) {
        File path = new File(path_string);
        File res;
        // If the path is absolute then return the absoluate reference
        if (path.isAbsolute()) {
            res = path;
        } else {
            // If the root path source is the jvm then just return the path.
            if (root_info != null &&
                    root_info.equals("jvm")) {
                return path;
            }
            // If the root path source is the configuration file then
            // concat the configuration path with the path string and return it.
            else {
                res = new File(root_path, path_string);
            }
        }
        return res;
    }

    /**
     * Sets up the log file from the config information.
     */
    private void setupLog(DBConfig config) {
        String log_path_string = config.getValue("log_path");
        String root_path_var = config.getValue("root_path");
        String read_only_access = config.getValue("read_only");
        String debug_logs = config.getValue("debug_logs");
        boolean read_only_bool = false;
        if (read_only_access != null) {
            read_only_bool = read_only_access.equalsIgnoreCase("enabled");
        }
        boolean debug_logs_bool = true;
        if (debug_logs != null) {
            debug_logs_bool = debug_logs.equalsIgnoreCase("enabled");
        }

        // Conditions for not initializing a log directory;
        //  1. read only access is enabled
        //  2. log_path is empty or not set

        if (debug_logs_bool && !read_only_bool &&
                log_path_string != null && !log_path_string.equals("")) {
            // First set up the debug information in this VM for the 'Debug' class.
            File log_path = parseFileString(config.currentPath(), root_path_var,
                    log_path_string);
            // If the path doesn't exist the make it.
            if (!log_path.exists()) {
                log_path.mkdirs();
            }
            // Set the log directory in the DatabaseSystem
            setLogDirectory(log_path);

            LogWriter f_writer;
            File debug_log_file;
            String dlog_file_name = "";
            try {
                dlog_file_name = config.getValue("debug_log_file");
                debug_log_file = new File(log_path.getCanonicalPath(),
                        dlog_file_name);

                // Allow log size to grow to 512k and allow 12 archives of the log
                f_writer = new LogWriter(debug_log_file, 512 * 1024, 12);
                f_writer.write("**** Debug log started: " +
                        new Date(System.currentTimeMillis()) + " ****\n");
                f_writer.flush();
            } catch (IOException e) {
                throw new RuntimeException(
                        "Unable to open debug file '" + dlog_file_name +
                                "' in path '" + log_path + "'");
            }
            setDebugOutput(f_writer);
        }

        // If 'debug_logs=disabled', don't write out any debug logs
        if (!debug_logs_bool) {
            // Otherwise set it up so the output from the logs goes to a PrintWriter
            // that doesn't do anything.  Basically - this means all log information
            // will get sent into a black hole.
            setDebugOutput(new PrintWriter(new Writer() {
                public void write(int c) throws IOException {
                }

                public void write(char[] cbuf, int off, int len) throws IOException {
                }

                public void write(String str, int off, int len) throws IOException {
                }

                public void flush() throws IOException {
                }

                public void close() throws IOException {
                }
            }));
        }

        int debug_level = Integer.parseInt(config.getValue("debug_level"));
        if (debug_level == -1) {
            setDebugLevel(255);
        } else {
            setDebugLevel(debug_level);
        }
    }

    /**
     * Returns a configuration value, or the default if it's not found.
     */
    public final String getConfigString(String property, String default_val) {
        String v = config.getValue(property);
        if (v == null) {
            return default_val;
        }
        return v.trim();
    }

    /**
     * Returns a configuration value, or the default if it's not found.
     */
    public final int getConfigInt(String property, int default_val) {
        String v = config.getValue(property);
        if (v == null) {
            return default_val;
        }
        return Integer.parseInt(v);
    }

    /**
     * Returns a configuration value, or the default if it's not found.
     */
    public final boolean getConfigBoolean(String property, boolean default_val) {
        String v = config.getValue(property);
        if (v == null) {
            return default_val;
        }
        return v.trim().equalsIgnoreCase("enabled");
    }


    /**
     * Given a regular expression string representing a particular library, this
     * will return the name of the class to use as a bridge between the library
     * and Pony.  Returns null if the library name is invalid.
     */
    private static String regexStringToClass(String lib) {
        if (lib.equals("java.util.regexp")) {
            return "com.pony.database.regexbridge.JavaRegex";
        } else if (lib.equals("org.apache.regexp")) {
            return "com.pony.database.regexbridge.ApacheRegex";
        } else if (lib.equals("gnu.regexp")) {
            return "com.pony.database.regexbridge.GNURegex";
        } else {
            return null;
        }
    }

    /**
     * Inits the TransactionSystem with the configuration properties of the
     * system.
     * This can only be called once, and should be called at database boot time.
     */
    public void init(DBConfig config) {

        function_factory_list = new ArrayList();
        function_lookup = new DSFunctionLookup();

        if (config != null) {
            this.config = config;

            // Set the read_only property
            read_only_access = getConfigBoolean("read_only", false);

            // Setup the log
            setupLog(config);

            // The storage encapsulation that has been configured.
            String storage_system = getConfigString("storage_system", "v1file");

            boolean is_file_store_mode;

            // Construct the system store.
            if (storage_system.equalsIgnoreCase("v1file")) {
                Debug().write(Lvl.MESSAGE, this,
                        "Storage System: v1 file storage mode.");

                // The path where the database data files are stored.
                String database_path = getConfigString("database_path", "./data");
                // The root path variable
                String root_path_var = getConfigString("root_path", "jvm");

                // Set the absolute database path
                db_path = parseFileString(config.currentPath(), root_path_var,
                        database_path);

                store_system = new V1FileStoreSystem(this, db_path, read_only_access);
                is_file_store_mode = true;
            } else if (storage_system.equalsIgnoreCase("v1javaheap")) {
                Debug().write(Lvl.MESSAGE, this,
                        "Storage System: v1 Java heap storage mode.");
                store_system = new V1HeapStoreSystem();
                is_file_store_mode = false;
            } else {
                String error_msg = "Unknown storage_system property: " + storage_system;
                Debug().write(Lvl.ERROR, this, error_msg);
                throw new RuntimeException(error_msg);
            }

            // Register the internal function factory,
            addFunctionFactory(new InternalFunctionFactory());

            String status;

            // Set up the DataCellCache from the values in the configuration
            int max_cache_size = 0, max_cache_entry_size = 0;

            max_cache_size = getConfigInt("data_cache_size", 0);
            max_cache_entry_size = getConfigInt("max_cache_entry_size", 0);

            if (max_cache_size >= 4096 &&
                    max_cache_entry_size >= 16 &&
                    max_cache_entry_size < (max_cache_size / 2)) {

                Debug().write(Lvl.MESSAGE, this,
                        "Internal Data Cache size:          " + max_cache_size);
                Debug().write(Lvl.MESSAGE, this,
                        "Internal Data Cache max cell size: " + max_cache_entry_size);

                // Find a prime hash size depending on the size of the cache.
                int hash_size = DataCellCache.closestPrime(max_cache_size / 55);

                // Set up the data_cell_cache
                data_cell_cache = new DataCellCache(this,
                        max_cache_size, max_cache_entry_size, hash_size);

            } else {
                Debug().write(Lvl.MESSAGE, this,
                        "Internal Data Cache disabled.");
            }

            // Are lookup comparison lists enabled?
//      lookup_comparison_list_enabled =
//                            getConfigBoolean("lookup_comparison_list", false);
            lookup_comparison_list_enabled = false;
            Debug().write(Lvl.MESSAGE, this,
                    "lookup_comparison_list = " + lookup_comparison_list_enabled);

            // Should we open the database in read only mode?
            Debug().write(Lvl.MESSAGE, this,
                    "read_only = " + read_only_access);
            if (read_only_access) stats.set(1, "DatabaseSystem.read_only");

//      // Hard Sync file system whenever we update index files?
//      if (is_file_store_mode) {
//        dont_synch_filesystem = getConfigBoolean("dont_synch_filesystem", false);
//        Debug().write(Lvl.MESSAGE, this,
//                      "dont_synch_filesystem = " + dont_synch_filesystem);
//      }

            // Generate transaction error if dirty selects are detected?
            transaction_error_on_dirty_select =
                    getConfigBoolean("transaction_error_on_dirty_select", true);
            Debug().write(Lvl.MESSAGE, this, "transaction_error_on_dirty_select = " +
                    transaction_error_on_dirty_select);

            // Case insensitive identifiers?
            ignore_case_for_identifiers =
                    getConfigBoolean("ignore_case_for_identifiers", false);
            Debug().write(Lvl.MESSAGE, this,
                    "ignore_case_for_identifiers = " + ignore_case_for_identifiers);

            // ---- Store system setup ----

            // See if this JVM supports the java.nio interface
            // (first introduced in 1.4)
            if (is_file_store_mode) {
                boolean nio_interface_available;
                try {
                    Class.forName("java.nio.channels.FileChannel");
                    nio_interface_available = true;
                    Debug().write(Lvl.MESSAGE, this,
                            "Java NIO API is available.");
                } catch (ClassNotFoundException e) {
                    nio_interface_available = false;
                    Debug().write(Lvl.MESSAGE, this,
                            "Java NIO API is not available.");
                }

                // Bug workaround - there are problems with memory mapped NIO under 95/98
                //   which we workaround by disabling NIO support on 95/98.
                boolean nio_bugged_os;
                String os_name = System.getProperties().getProperty("os.name");
                nio_bugged_os = (os_name.equalsIgnoreCase("Windows 95") ||
                        os_name.equalsIgnoreCase("Windows 98"));

                // Get the safety level of the file system where 10 is the most safe
                // and 1 is the least safe.
                int io_safety_level = getConfigInt("io_safety_level", 10);
                if (io_safety_level < 1 || io_safety_level > 10) {
                    Debug().write(Lvl.MESSAGE, this,
                            "Invalid io_safety_level value.  Setting to the most safe level.");
                    io_safety_level = 10;
                }
                Debug().write(Lvl.MESSAGE, this,
                        "io_safety_level = " + io_safety_level);

                // Logging is disabled when safety level is less or equal to 2
                boolean enable_logging = true;
                if (io_safety_level <= 2) {
                    Debug().write(Lvl.MESSAGE, this,
                            "Disabling journaling and file sync.");
                    enable_logging = false;
                }

                // If the configuration property 'use_nio_if_available' is enabled then
                // we setup a LoggingBufferManager that uses NIO (default to 'false')
                boolean use_nio_if_available =
                        getConfigBoolean("use_nio_if_available", false);
                boolean force_use_nio = getConfigBoolean("force_use_nio", false);

                String api_to_use;
                int page_size;
                int max_pages;

                final boolean disable_nio = true;

                // If NIO interface available and configuration tells us to use NIO and
                // we are not running on an OS where NIO is buggy, we set the NIO options
                // here.
                if (!disable_nio &&
                        (force_use_nio ||
                                (nio_interface_available &&
                                        use_nio_if_available &&
                                        !nio_bugged_os))) {
                    Debug().write(Lvl.MESSAGE, this,
                            "Using NIO API for OS memory mapped file access.");
                    page_size = getConfigInt("buffered_nio_page_size", 1024 * 1024);
                    max_pages = getConfigInt("buffered_nio_max_pages", 64);
                    api_to_use = "Java NIO";
                } else {
                    Debug().write(Lvl.MESSAGE, this,
                            "Using stardard IO API for heap buffered file access.");
                    page_size = getConfigInt("buffered_io_page_size", 8192);
                    max_pages = getConfigInt("buffered_io_max_pages", 256);
                    api_to_use = "Java IO";
                }

                // Output this information to the log
                Debug().write(Lvl.MESSAGE, this,
                        "[Buffer Manager] Using IO API: " + api_to_use);
                Debug().write(Lvl.MESSAGE, this,
                        "[Buffer Manager] Page Size: " + page_size);
                Debug().write(Lvl.MESSAGE, this,
                        "[Buffer Manager] Max pages: " + max_pages);

                // Journal path is currently always the same as database path.
                final File journal_path = db_path;
                // Max slice size is 1 GB for file scattering class
                final long max_slice_size = 16384 * 65536;
                // First file extention is 'pony'
                final String first_file_ext = "pony";

                // Set up the BufferManager
                buffer_manager = new LoggingBufferManager(
                        db_path, journal_path, read_only_access, max_pages, page_size,
                        first_file_ext, max_slice_size, Debug(), enable_logging);
                // ^ This is a big constructor.  It sets up the logging manager and
                //   sets a resource store data accessor converter to a scattering
                //   implementation with a max slice size of 1 GB

                // Start the buffer manager.
                try {
                    buffer_manager.start();
                } catch (IOException e) {
                    Debug().write(Lvl.ERROR, this, "Error starting buffer manager");
                    Debug().writeException(Lvl.ERROR, e);
                    throw new Error("IO Error: " + e.getMessage());
                }

            }

            // What regular expression library are we using?
            // If we want the engine to support other regular expression libraries
            // then include the additional entries here.

            // Test to see if the regex API exists
            boolean regex_api_exists;
            try {
                Class.forName("java.util.regex.Pattern");
                regex_api_exists = true;
            } catch (ClassNotFoundException e) {
                // Internal API doesn't exist
                regex_api_exists = false;
                Debug().write(Lvl.MESSAGE, this,
                        "Java regex API not available.");
            }

            String regex_bridge;
            String lib_used;

            String force_lib = getConfigString("force_regex_library", null);

            // Are we forcing a particular regular expression library?
            if (force_lib != null) {
                lib_used = force_lib;
                // Convert the library string to a class name
                regex_bridge = regexStringToClass(force_lib);
            } else {
                String lib = getConfigString("regex_library", null);
                lib_used = lib;
                // Use the standard Java 1.4 regular expression library if it is found.
                if (regex_api_exists) {
                    regex_bridge = "com.pony.database.regexbridge.JavaRegex";
                    lib_used = "java.util.regexp";
                }
//        else if (lib_used != null) {
//          regex_bridge = regexStringToClass(lib_used);
//        }
                else {
                    regex_bridge = null;
                    lib_used = null;
                }
            }

            if (regex_bridge != null) {
                try {
                    Class c = Class.forName(regex_bridge);
                    regex_library = (RegexLibrary) c.newInstance();
                    Debug().write(Lvl.MESSAGE, this,
                            "Using regex bridge: " + lib_used);
                } catch (Throwable e) {
                    Debug().write(Lvl.ERROR, this,
                            "Unable to load regex bridge: " + regex_bridge);
                    Debug().writeException(Lvl.WARNING, e);
                }
            } else {
                if (lib_used != null) {
                    Debug().write(Lvl.ERROR, this,
                            "Regex library not known: " + lib_used);
                }
                Debug().write(Lvl.MESSAGE, this,
                        "Regex features disabled.");
            }

            // ---------- Plug ins ---------

            try {
                // The 'function_factories' property.
                String function_factories =
                        getConfigString("function_factories", null);
                if (function_factories != null) {
                    List factories = StringUtil.explode(function_factories, ";");
                    for (int i = 0; i < factories.size(); ++i) {
                        String factory_class = factories.get(i).toString();
                        Class c = Class.forName(factory_class);
                        FunctionFactory fun_factory = (FunctionFactory) c.newInstance();
                        addFunctionFactory(fun_factory);
                        Debug().write(Lvl.MESSAGE, this,
                                "Successfully added function factory: " + factory_class);
                    }
                } else {
                    Debug().write(Lvl.MESSAGE, this,
                            "No 'function_factories' config property found.");
                    // If resource missing, do nothing...
                }
            } catch (Throwable e) {
                Debug().write(Lvl.ERROR, this,
                        "Error parsing 'function_factories' configuration property.");
                Debug().writeException(e);
            }

            // Flush the contents of the function lookup object.
            flushCachedFunctionLookup();

        }

    }

    /**
     * Hack - set up the DataCellCache in DatabaseSystem so we can use the
     * MasterTableDataSource object without having to boot a new DatabaseSystem.
     */
    public void setupRowCache(int max_cache_size,
                              int max_cache_entry_size) {
        // Set up the data_cell_cache
        data_cell_cache =
                new DataCellCache(this, max_cache_size, max_cache_entry_size);
    }

    /**
     * Returns true if the database is in read only mode.  In read only mode,
     * any 'write' operations are not permitted.
     */
    public boolean readOnlyAccess() {
        return read_only_access;
    }

    /**
     * Returns the path of the database in the local file system if the database
     * exists within the local file system.  If the database is not within the
     * local file system then null is returned.  It is recommended this method
     * is not used unless for legacy or compatability purposes.
     */
    public File getDatabasePath() {
        return db_path;
    }

    /**
     * Returns true if the database should perform checking of table locks.
     */
    public boolean tableLockingEnabled() {
        boolean table_lock_check = false;
        return table_lock_check;
    }

    /**
     * Returns true if we should generate lookup caches in InsertSearch otherwise
     * returns false.
     */
    public boolean lookupComparisonListEnabled() {
        return lookup_comparison_list_enabled;
    }

    /**
     * Returns true if all table indices are kept behind a soft reference that
     * can be garbage collected.
     */
    public boolean softIndexStorage() {
        boolean soft_index_storage = false;
        return soft_index_storage;
    }

    /**
     * Returns the status of the 'always_reindex_dirty_tables' property.
     */
    public boolean alwaysReindexDirtyTables() {
        boolean always_reindex_dirty_tables = false;
        return always_reindex_dirty_tables;
    }

    /**
     * Returns true if we shouldn't synchronize with the file system when
     * important indexing information is flushed to the disk.
     */
    public boolean dontSynchFileSystem() {
        boolean dont_synch_filesystem = false;
        return dont_synch_filesystem;
    }

    /**
     * Returns true if during commit the engine should look for any selects
     * on a modified table and fail if they are detected.
     */
    public boolean transactionErrorOnDirtySelect() {
        return transaction_error_on_dirty_select;
    }

    /**
     * Returns true if the parser should ignore case when searching for
     * schema/table/column identifiers.
     */
    public boolean ignoreIdentifierCase() {
        return ignore_case_for_identifiers;
    }

    /**
     * Returns the LoggingBufferManager object enabling us to create no file
     * stores in the file system.  This provides access to the buffer scheme that
     * has been configured.
     */
    public LoggingBufferManager getBufferManager() {
        return buffer_manager;
    }

    /**
     * Returns the regular expression library from the configuration file.
     */
    public RegexLibrary getRegexLibrary() {
        if (regex_library != null) {
            return regex_library;
        }
        throw new Error("No regular expression library found in classpath " +
                "and/or in configuration file.");
    }

    // ---------- Store System encapsulation ----------

    /**
     * Returns the StoreSystem encapsulation being used in this database.
     */
    public final StoreSystem storeSystem() {
        return store_system;
    }

    // ---------- Debug logger methods ----------

    /**
     * Sets the Writer output for the debug logger.
     */
    public final void setDebugOutput(java.io.Writer writer) {
//    System.out.println("**** Setting debug log output ****" + writer);
//    System.out.println(logger);
        logger.setOutput(writer);
    }

    /**
     * Sets the debug minimum level that is output to the logger.
     */
    public final void setDebugLevel(int level) {
        logger.setDebugLevel(level);
    }

    /**
     * Returns the DebugLogger object that is used to log debug message.  This
     * method must always return a debug logger that we can log to.
     */
    public final DebugLogger Debug() {
        return logger;
    }

    // ---------- Function factories ----------

    /**
     * Registers a new FunctionFactory with the database system.  The function
     * factories are used to resolve a function name into a Function object.
     * Function factories are checked in the order they are added to the database
     * system.
     */
    public void addFunctionFactory(FunctionFactory factory) {
        synchronized (function_factory_list) {
            function_factory_list.add(factory);
        }
        factory.init();
    }

    /**
     * Flushes the 'FunctionLookup' object returned by the getFunctionLookup
     * method.  This should be called if the function factory list has been
     * modified in some way.
     */
    public void flushCachedFunctionLookup() {
        FunctionFactory[] factories;
        synchronized (function_factory_list) {
            factories = (FunctionFactory[]) function_factory_list.toArray(
                    new FunctionFactory[function_factory_list.size()]);
        }
        function_lookup.flushContents(factories);
    }

    /**
     * Returns a FunctionLookup object that will search through the function
     * factories in this database system and find and resolve a function.  The
     * returned object may throw an exception from the 'generateFunction' method
     * if the FunctionDef is invalid.  For example, if the number of parameters
     * is incorrect or the name can not be found.
     */
    public FunctionLookup getFunctionLookup() {
        return function_lookup;
    }

    // ---------- System preparers ----------

    /**
     * Given a Transaction.CheckExpression, this will prepare the expression and
     * return a new prepared CheckExpression.  The default implementation of this
     * is to do nothing.  However, a sub-class of the system choose to prepare
     * the expression, such as resolving the functions via the function lookup,
     * and resolving the sub-queries, etc.
     */
    public Transaction.CheckExpression prepareTransactionCheckConstraint(
            DataTableDef table_def, Transaction.CheckExpression check) {

//    ExpressionPreparer expression_preparer = getFunctionExpressionPreparer();
        // Resolve the expression to this table and row and evaluate the
        // check constraint.
        Expression exp = check.expression;
        table_def.resolveColumns(ignoreIdentifierCase(), exp);
//    try {
//      // Prepare the functions
//      exp.prepare(expression_preparer);
//    }
//    catch (Exception e) {
//      Debug().writeException(e);
//      throw new RuntimeException(e.getMessage());
//    }

        return check;
    }

    // ---------- Database System Statistics Methods ----------

    /**
     * Returns a com.pony.util.Stats object that can be used to keep track
     * of database statistics for this VM.
     */
    public final Stats stats() {
        return stats;
    }

    // ---------- Log directory management ----------

    /**
     * Sets the log directory.  This should preferably be called during
     * initialization.  If the log directory is not set or is set to 'null' then
     * no logging to files occurs.
     */
    public final void setLogDirectory(File log_path) {
        this.log_directory = log_path;
    }

    /**
     * Returns the current log directory or null if no logging should occur.
     */
    public final File getLogDirectory() {
        return log_directory;
    }

    // ---------- Cache Methods ----------

    /**
     * Returns a DataCellCache object that is a shared resource between all
     * database's running on this VM.  If this returns 'null' then the internal
     * cache is disabled.
     */
    DataCellCache getDataCellCache() {
        return data_cell_cache;
    }

    // ---------- Dispatch methods ----------

    /**
     * The dispatcher.
     */
    private DatabaseDispatcher dispatcher;

    /**
     * Returns the DatabaseDispatcher object.
     */
    private DatabaseDispatcher getDispatcher() {
        synchronized (this) {
            if (dispatcher == null) {
                dispatcher = new DatabaseDispatcher(this);
            }
            return dispatcher;
        }
    }

    /**
     * Creates an event object that is passed into 'postEvent' method
     * to run the given Runnable method after the time has passed.
     * <p>
     * The event created here can be safely posted on the event queue as many
     * times as you like.  It's useful to create an event as a persistant object
     * to service some event.  Just post it on the dispatcher when you want
     * it run!
     */
    Object createEvent(Runnable runnable) {
        return getDispatcher().createEvent(runnable);
    }

    /**
     * Adds a new event to be dispatched on the queue after 'time_to_wait'
     * milliseconds has passed.
     * <p>
     * 'event' must be an event object returned via 'createEvent'.
     */
    void postEvent(int time_to_wait, Object event) {
        getDispatcher().postEvent(time_to_wait, event);
    }


    /**
     * Disposes this object.
     */
    public void dispose() {
        if (buffer_manager != null) {
            try {
                // Set a check point
                store_system.setCheckPoint();
                // Stop the buffer manager
                buffer_manager.stop();
            } catch (IOException e) {
                System.out.println("Error stopping buffer manager.");
                e.printStackTrace();
            }
        }
        buffer_manager = null;
        regex_library = null;
        data_cell_cache = null;
        config = null;
        log_directory = null;
        function_factory_list = null;
        store_system = null;
        if (dispatcher != null) {
            dispatcher.finish();
        }
//    trigger_manager = null;
        dispatcher = null;
    }


    // ---------- Inner classes ----------

    /**
     * A FunctionLookup implementation that will look up a function from a
     * list of FunctionFactory objects provided with.
     */
    private static class DSFunctionLookup implements FunctionLookup {

        private FunctionFactory[] factories;

        public synchronized Function generateFunction(FunctionDef function_def) {
            for (int i = 0; i < factories.length; ++i) {
                Function f = factories[i].generateFunction(function_def);
                if (f != null) {
                    return f;
                }
            }
            return null;
        }

        public synchronized boolean isAggregate(FunctionDef function_def) {
            for (int i = 0; i < factories.length; ++i) {
                FunctionInfo f_info =
                        factories[i].getFunctionInfo(function_def.getName());
                if (f_info != null) {
                    return f_info.getType() == FunctionInfo.AGGREGATE;
                }
            }
            return false;
        }

        public synchronized void flushContents(FunctionFactory[] factories) {
            this.factories = factories;
        }

    }

}
