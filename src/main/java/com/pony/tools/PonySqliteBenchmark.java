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

package com.pony.tools;

import com.pony.database.control.DBController;
import com.pony.database.control.DBSystem;
import com.pony.database.control.DefaultDBConfig;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * A small command line benchmark runner for comparing PonySQL and SQLite through
 * JDBC on identical synthetic datasets.
 */
public final class PonySqliteBenchmark {

    private static final List<Integer> DEFAULT_ROW_COUNTS =
            Arrays.asList(10_000, 100_000);

    private static final List<String> DEFAULT_SCENARIOS =
            Arrays.asList("bulk_insert", "sequential_read", "indexed_point",
                    "indexed_range", "composite_lookup");

    private static final int DEFAULT_COMMIT_EVERY = 10_000;
    private static final int DEFAULT_LOOKUP_REPETITIONS = 1_000;
    private static final DateTimeFormatter RUN_ID_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private PonySqliteBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        BenchmarkOptions options = BenchmarkOptions.parse(args);
        if (options.help) {
            printUsage();
            return;
        }

        Files.createDirectories(options.outputDirectory);
        Path runDirectory = options.outputDirectory.resolve(
                "run-" + RUN_ID_FORMAT.format(LocalDateTime.now()));
        Files.createDirectories(runDirectory);

        List<BenchmarkResult> results = new ArrayList<>();
        for (int rows : options.rowCounts) {
            for (String engine : options.engines) {
                Path databaseDirectory = runDirectory.resolve(engine + "-" + rows);
                Files.createDirectories(databaseDirectory);
                BenchmarkDatabase database = openDatabase(engine, databaseDirectory);
                try {
                    results.addAll(runForDatabase(database, rows, options));
                } finally {
                    database.close();
                    if (!options.keepDatabases) {
                        deleteRecursively(databaseDirectory);
                    }
                }
            }
        }

        writeReports(runDirectory, results);
        System.out.println("Benchmark results written to " + runDirectory);
    }

    private static BenchmarkDatabase openDatabase(String engine, Path directory)
            throws Exception {
        if ("pony".equals(engine)) {
            DefaultDBConfig config = new DefaultDBConfig();
            config.setDatabasePath(directory.resolve("data").toString());
            config.setLogPath(directory.resolve("log").toString());
            DBSystem database = DBController.getDefault()
                    .createDatabase(config, "test", "test");
            Connection connection = database.getConnection("test", "test");
            return new BenchmarkDatabase(engine, connection, database);
        } else if ("sqlite".equals(engine)) {
            Class.forName("org.sqlite.JDBC");
            Path databaseFile = directory.resolve("sqlite.db");
            Connection connection = DriverManager.getConnection(
                    "jdbc:sqlite:" + databaseFile);
            return new BenchmarkDatabase(engine, connection, null);
        } else {
            throw new IllegalArgumentException("Unsupported engine: " + engine);
        }
    }

    private static List<BenchmarkResult> runForDatabase(
            BenchmarkDatabase database, int rows, BenchmarkOptions options)
            throws SQLException {
        List<BenchmarkResult> results = new ArrayList<>();
        Connection connection = database.connection;

        createSchema(connection);

        long insertStart = System.nanoTime();
        insertRows(connection, rows, options.commitEvery);
        long insertElapsed = elapsedMillis(insertStart);
        if (options.scenarios.contains("bulk_insert")) {
            results.add(new BenchmarkResult(database.engine, "bulk_insert",
                    rows, rows, insertElapsed));
        }

        createIndexes(connection);

        if (options.scenarios.contains("sequential_read")) {
            results.add(timeScenario(database.engine, "sequential_read", rows, rows,
                    () -> sequentialRead(connection)));
        }
        if (options.scenarios.contains("indexed_point")) {
            int repetitions = Math.min(options.lookupRepetitions, rows);
            results.add(timeScenario(database.engine, "indexed_point", rows,
                    repetitions, () -> indexedPointLookup(connection, rows,
                            repetitions)));
        }
        if (options.scenarios.contains("indexed_range")) {
            int repetitions = Math.min(options.lookupRepetitions, rows);
            results.add(timeScenario(database.engine, "indexed_range", rows,
                    repetitions, () -> indexedRangeLookup(connection, rows,
                            repetitions)));
        }
        if (options.scenarios.contains("composite_lookup")) {
            int repetitions = Math.min(options.lookupRepetitions, rows);
            results.add(timeScenario(database.engine, "composite_lookup", rows,
                    repetitions, () -> compositeLookup(connection, repetitions)));
        }

        return results;
    }

    private static void createSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE bench_data (" +
                            " id INTEGER, " +
                            " group_id INTEGER, " +
                            " bucket INTEGER, " +
                            " payload VARCHAR(64) )");
        }
    }

    private static void createIndexes(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE INDEX idx_bench_id ON bench_data(id)");
            statement.executeUpdate(
                    "CREATE INDEX idx_bench_group_bucket ON bench_data(group_id, bucket)");
        }
    }

    private static void insertRows(Connection connection, int rows, int commitEvery)
            throws SQLException {
        boolean previousAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO bench_data (id, group_id, bucket, payload) " +
                        "VALUES (?, ?, ?, ?)")) {
            for (int i = 0; i < rows; ++i) {
                insert.setInt(1, i);
                insert.setInt(2, i % 10_000);
                insert.setInt(3, i % 128);
                insert.setString(4, "payload-" + i);
                insert.executeUpdate();
                if (commitEvery > 0 && (i + 1) % commitEvery == 0) {
                    connection.commit();
                }
            }
            connection.commit();
        } finally {
            connection.setAutoCommit(previousAutoCommit);
        }
    }

    private static void sequentialRead(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT id, group_id, bucket, payload FROM bench_data")) {
            long checksum = 0;
            while (result.next()) {
                checksum += result.getInt(1);
                checksum += result.getInt(2);
                checksum += result.getInt(3);
                String payload = result.getString(4);
                if (payload != null) {
                    checksum += payload.length();
                }
            }
            consume(checksum);
        }
    }

    private static void indexedPointLookup(Connection connection, int rows,
                                           int repetitions) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT payload FROM bench_data WHERE id = ?")) {
            long checksum = 0;
            for (int i = 0; i < repetitions; ++i) {
                select.setInt(1, deterministicRow(i, rows));
                try (ResultSet result = select.executeQuery()) {
                    while (result.next()) {
                        String payload = result.getString(1);
                        if (payload != null) {
                            checksum += payload.length();
                        }
                    }
                }
            }
            consume(checksum);
        }
    }

    private static void indexedRangeLookup(Connection connection, int rows,
                                           int repetitions) throws SQLException {
        int rangeSize = Math.max(1, Math.min(1_000, rows / 100));
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT COUNT(*) FROM bench_data WHERE id >= ? AND id < ?")) {
            long checksum = 0;
            for (int i = 0; i < repetitions; ++i) {
                int start = deterministicRow(i, Math.max(1, rows - rangeSize));
                select.setInt(1, start);
                select.setInt(2, start + rangeSize);
                try (ResultSet result = select.executeQuery()) {
                    if (result.next()) {
                        checksum += result.getLong(1);
                    }
                }
            }
            consume(checksum);
        }
    }

    private static void compositeLookup(Connection connection, int repetitions)
            throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT COUNT(*) FROM bench_data WHERE group_id = ? AND bucket = ?")) {
            long checksum = 0;
            for (int i = 0; i < repetitions; ++i) {
                select.setInt(1, i % 10_000);
                select.setInt(2, i % 128);
                try (ResultSet result = select.executeQuery()) {
                    if (result.next()) {
                        checksum += result.getLong(1);
                    }
                }
            }
            consume(checksum);
        }
    }

    private static BenchmarkResult timeScenario(String engine, String scenario,
                                                int rows, long operations,
                                                SqlRunnable runnable)
            throws SQLException {
        long start = System.nanoTime();
        runnable.run();
        return new BenchmarkResult(engine, scenario, rows, operations,
                elapsedMillis(start));
    }

    private static int deterministicRow(int i, int rows) {
        return (int) (((long) i * 7_919L) % rows);
    }

    private static long elapsedMillis(long startNanos) {
        long elapsedNanos = System.nanoTime() - startNanos;
        return Math.max(1L, elapsedNanos / 1_000_000L);
    }

    private static void consume(long checksum) {
        if (checksum == Long.MIN_VALUE) {
            System.out.println("unreachable checksum: " + checksum);
        }
    }

    private static void writeReports(Path runDirectory,
                                     List<BenchmarkResult> results)
            throws IOException {
        Path csv = runDirectory.resolve("benchmark-results.csv");
        try (BufferedWriter writer = Files.newBufferedWriter(csv)) {
            writer.write("engine,scenario,rows,operations,elapsed_ms,ops_per_second");
            writer.newLine();
            for (BenchmarkResult result : results) {
                writer.write(result.toCsv());
                writer.newLine();
            }
        }

        Path markdown = runDirectory.resolve("benchmark-results.md");
        try (BufferedWriter writer = Files.newBufferedWriter(markdown)) {
            writer.write("# PonySQL vs SQLite benchmark results");
            writer.newLine();
            writer.newLine();
            writer.write("| Engine | Scenario | Rows | Operations | Elapsed ms | Ops/sec |");
            writer.newLine();
            writer.write("| --- | --- | ---: | ---: | ---: | ---: |");
            writer.newLine();
            for (BenchmarkResult result : results) {
                writer.write(result.toMarkdownRow());
                writer.newLine();
            }
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(PonySqliteBenchmark::deletePath);
        }
    }

    private static void deletePath(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new RuntimeException("Unable to delete " + path, e);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: mvn -Pbenchmarks -DskipTests compile exec:java " +
                "-Dexec.args=\"[options]\"");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --rows 10000,100000,1000000,10000000");
        System.out.println("  --engines pony,sqlite");
        System.out.println("  --scenarios bulk_insert,sequential_read,indexed_point,indexed_range,composite_lookup");
        System.out.println("  --out target/benchmarks");
        System.out.println("  --commit-every 10000");
        System.out.println("  --lookup-repetitions 1000");
        System.out.println("  --keep-databases");
        System.out.println("  --help");
    }

    private static final class BenchmarkOptions {
        private List<Integer> rowCounts = DEFAULT_ROW_COUNTS;
        private List<String> engines = Arrays.asList("pony", "sqlite");
        private List<String> scenarios = DEFAULT_SCENARIOS;
        private Path outputDirectory = Path.of("target", "benchmarks");
        private int commitEvery = DEFAULT_COMMIT_EVERY;
        private int lookupRepetitions = DEFAULT_LOOKUP_REPETITIONS;
        private boolean keepDatabases = false;
        private boolean help = false;

        private static BenchmarkOptions parse(String[] args) {
            BenchmarkOptions options = new BenchmarkOptions();
            for (int i = 0; i < args.length; ++i) {
                String arg = args[i];
                switch (arg) {
                    case "--help":
                    case "-h":
                        options.help = true;
                        break;
                    case "--rows":
                        options.rowCounts = parseIntegers(nextValue(args, ++i, arg));
                        break;
                    case "--engines":
                        options.engines = parseStrings(nextValue(args, ++i, arg));
                        break;
                    case "--scenarios":
                        options.scenarios = parseStrings(nextValue(args, ++i, arg));
                        break;
                    case "--out":
                        options.outputDirectory = Path.of(nextValue(args, ++i, arg));
                        break;
                    case "--commit-every":
                        options.commitEvery = Integer.parseInt(
                                nextValue(args, ++i, arg));
                        break;
                    case "--lookup-repetitions":
                        options.lookupRepetitions = Integer.parseInt(
                                nextValue(args, ++i, arg));
                        break;
                    case "--keep-databases":
                        options.keepDatabases = true;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown option: " + arg);
                }
            }
            validateOptions(options);
            return options;
        }

        private static String nextValue(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            return args[index];
        }

        private static List<Integer> parseIntegers(String value) {
            List<Integer> result = new ArrayList<>();
            for (String part : value.split(",")) {
                result.add(Integer.parseInt(part.trim()));
            }
            return result;
        }

        private static List<String> parseStrings(String value) {
            List<String> result = new ArrayList<>();
            for (String part : value.split(",")) {
                result.add(part.trim().toLowerCase(Locale.ROOT));
            }
            return result;
        }

        private static void validateOptions(BenchmarkOptions options) {
            for (Integer rows : options.rowCounts) {
                if (rows == null || rows <= 0) {
                    throw new IllegalArgumentException("Rows must be positive.");
                }
            }
            for (String engine : options.engines) {
                if (!"pony".equals(engine) && !"sqlite".equals(engine)) {
                    throw new IllegalArgumentException("Unsupported engine: " + engine);
                }
            }
            for (String scenario : options.scenarios) {
                if (!DEFAULT_SCENARIOS.contains(scenario)) {
                    throw new IllegalArgumentException(
                            "Unsupported scenario: " + scenario);
                }
            }
        }
    }

    private static final class BenchmarkDatabase implements AutoCloseable {
        private final String engine;
        private final Connection connection;
        private final DBSystem ponyDatabase;

        private BenchmarkDatabase(String engine, Connection connection,
                                  DBSystem ponyDatabase) {
            this.engine = engine;
            this.connection = connection;
            this.ponyDatabase = ponyDatabase;
        }

        public void close() throws Exception {
            connection.close();
            if (ponyDatabase != null) {
                ponyDatabase.close();
            }
        }
    }

    private static final class BenchmarkResult {
        private final String engine;
        private final String scenario;
        private final int rows;
        private final long operations;
        private final long elapsedMillis;

        private BenchmarkResult(String engine, String scenario, int rows,
                                long operations, long elapsedMillis) {
            this.engine = engine;
            this.scenario = scenario;
            this.rows = rows;
            this.operations = operations;
            this.elapsedMillis = elapsedMillis;
        }

        private double operationsPerSecond() {
            return (operations * 1000.0d) / elapsedMillis;
        }

        private String toCsv() {
            return engine + "," + scenario + "," + rows + "," + operations + "," +
                    elapsedMillis + "," +
                    String.format(Locale.ROOT, "%.2f", operationsPerSecond());
        }

        private String toMarkdownRow() {
            return "| " + engine + " | " + scenario + " | " + rows + " | " +
                    operations + " | " + elapsedMillis + " | " +
                    String.format(Locale.ROOT, "%.2f", operationsPerSecond()) + " |";
        }
    }

    private interface SqlRunnable {
        void run() throws SQLException;
    }
}
