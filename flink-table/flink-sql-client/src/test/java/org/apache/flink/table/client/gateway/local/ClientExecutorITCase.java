/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.gateway.ClientResult;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ExecutorImpl;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.local.result.ChangelogCollectResult;
import org.apache.flink.table.client.gateway.local.result.MaterializedResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointExtension;
import org.apache.flink.table.gateway.service.utils.SqlGatewayServiceExtension;
import org.apache.flink.table.utils.UserDefinedFunctions;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_MAX_TABLE_RESULT_ROWS;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Contains basic tests for the {@link ExecutorImpl}. */
class ExecutorImplITCase {

    private static final int NUM_TMS = 2;
    private static final int NUM_SLOTS_PER_TM = 2;

    @TempDir
    @Order(1)
    public static File tempFolder;

    @RegisterExtension
    @Order(2)
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    () ->
                            new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(getConfig())
                                    .setNumberTaskManagers(NUM_TMS)
                                    .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
                                    .build());

    @RegisterExtension
    @Order(3)
    public static final SqlGatewayServiceExtension SQL_GATEWAY_SERVICE_EXTENSION =
            new SqlGatewayServiceExtension(MINI_CLUSTER_RESOURCE::getClientConfiguration);

    @RegisterExtension
    @Order(4)
    private static final SqlGatewayRestEndpointExtension SQL_GATEWAY_REST_ENDPOINT_EXTENSION =
            new SqlGatewayRestEndpointExtension(SQL_GATEWAY_SERVICE_EXTENSION::getService);

    private static RestClusterClient<?> clusterClient;

    // a generated UDF jar used for testing classloading of dependencies
    private static URL udfDependency;

    @BeforeAll
    static void setup(@InjectClusterClient RestClusterClient<?> injectedClusterClient)
            throws Exception {
        clusterClient = injectedClusterClient;
        File udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        tempFolder,
                        "test-classloader-udf.jar",
                        GENERATED_LOWER_UDF_CLASS,
                        String.format(GENERATED_LOWER_UDF_CODE, GENERATED_LOWER_UDF_CLASS));
        udfDependency = udfJar.toURI().toURL();
    }

    private static Configuration getConfig() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
        config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, tempFolder.toURI().toString());
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, tempFolder.toURI().toString());
        return config;
    }

    @Test
    void testCompleteStatement() {
        final Executor executor = createLocalExecutor();
        executor.openSession("test-session");
        initSession(executor, Collections.emptyMap());

        final List<String> expectedTableHints =
                Arrays.asList(
                        "default_catalog.default_database.TableNumber1",
                        "default_catalog.default_database.TableSourceSink");
        assertThat(executor.completeStatement("SELECT * FROM Ta", 16))
                .isEqualTo(expectedTableHints);

        final List<String> expectedClause = Collections.singletonList("WHERE");
        assertThat(executor.completeStatement("SELECT * FROM TableNumber1 WH", 29))
                .isEqualTo(expectedClause);

        final List<String> expectedField = Collections.singletonList("IntegerField1");
        assertThat(executor.completeStatement("SELECT * FROM TableNumber1 WHERE Inte", 37))
                .isEqualTo(expectedField);
        executor.closeSession();
    }

    @Test
    void testStreamQueryExecutionChangelog() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        Configuration configuration = Configuration.fromMap(getDefaultSessionConfigMap());

        final Executor executor =
                createLocalExecutor(Collections.singletonList(udfDependency), configuration);
        executor.openSession("test-session");

        initSession(executor, replaceVars);
        try {
            // start job and retrieval
            final ResultDescriptor desc =
                    executeQuery(
                            executor,
                            "SELECT scalarUDF(IntegerField1, 5), StringField1, 'ABC' FROM TableNumber1");

            assertThat(desc.isMaterialized()).isFalse();

            final List<String> actualResults =
                    retrieveChangelogResult(desc.createResult(), desc.getRowDataStringConverter());

            final List<String> expectedResults = new ArrayList<>();
            expectedResults.add("[47, Hello World, ABC]");
            expectedResults.add("[27, Hello World, ABC]");
            expectedResults.add("[37, Hello World, ABC]");
            expectedResults.add("[37, Hello World, ABC]");
            expectedResults.add("[47, Hello World, ABC]");
            expectedResults.add("[57, Hello World!!!!, ABC]");

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        } finally {
            executor.closeSession();
        }
    }

    @Test
    void testStreamQueryExecutionChangelogMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        Configuration configuration = Configuration.fromMap(getDefaultSessionConfigMap());

        final Executor executor =
                createLocalExecutor(Collections.singletonList(udfDependency), configuration);
        executor.openSession("test-session");

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("[47, Hello World]");
        expectedResults.add("[27, Hello World]");
        expectedResults.add("[37, Hello World]");
        expectedResults.add("[37, Hello World]");
        expectedResults.add("[47, Hello World]");
        expectedResults.add("[57, Hello World!!!!]");

        initSession(executor, replaceVars);
        try {
            for (int i = 0; i < 3; i++) {
                // start job and retrieval
                final ResultDescriptor desc =
                        executeQuery(
                                executor,
                                "SELECT scalarUDF(IntegerField1, 5), StringField1 FROM TableNumber1");

                assertThat(desc.isMaterialized()).isFalse();

                final List<String> actualResults =
                        retrieveChangelogResult(
                                desc.createResult(), desc.getRowDataStringConverter());

                TestBaseUtils.compareResultCollections(
                        expectedResults, actualResults, Comparator.naturalOrder());
            }
        } finally {
            executor.closeSession();
        }
    }

    @Test
    void testStreamQueryExecutionTable() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        Map<String, String> configMap = getDefaultSessionConfigMap();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());

        final String query =
                "SELECT scalarUDF(IntegerField1, 5), StringField1, 'ABC' FROM TableNumber1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("[47, Hello World, ABC]");
        expectedResults.add("[27, Hello World, ABC]");
        expectedResults.add("[37, Hello World, ABC]");
        expectedResults.add("[37, Hello World, ABC]");
        expectedResults.add("[47, Hello World, ABC]");
        expectedResults.add("[57, Hello World!!!!, ABC]");

        executeStreamQueryTable(replaceVars, configMap, query, expectedResults);
    }

    @Test
    void testStreamQueryExecutionTableMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());

        final String query = "SELECT scalarUDF(IntegerField1, 5), StringField1 FROM TableNumber1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("[47, Hello World]");
        expectedResults.add("[27, Hello World]");
        expectedResults.add("[37, Hello World]");
        expectedResults.add("[37, Hello World]");
        expectedResults.add("[47, Hello World]");
        expectedResults.add("[57, Hello World!!!!]");

        for (int i = 0; i < 3; i++) {
            executeStreamQueryTable(replaceVars, configMap, query, expectedResults);
        }
    }

    @Test
    void testStreamQueryExecutionLimitedTable() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);

        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(EXECUTION_MAX_TABLE_RESULT_ROWS.key(), "1");

        final String query =
                "SELECT COUNT(*), StringField1 FROM TableNumber1 GROUP BY StringField1";

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("[1, Hello World!!!!]");

        executeStreamQueryTable(replaceVars, configMap, query, expectedResults);
    }

    @Test
    void testBatchQueryExecution() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.BATCH.name());

        final Executor executor =
                createLocalExecutor(
                        Collections.singletonList(udfDependency), Configuration.fromMap(configMap));
        executor.openSession("test-session");

        initSession(executor, replaceVars);
        try {
            final ResultDescriptor desc = executeQuery(executor, "SELECT *, 'ABC' FROM TestView1");

            assertThat(desc.isMaterialized()).isTrue();

            final List<String> actualResults =
                    retrieveTableResult(desc.createResult(), desc.getRowDataStringConverter());

            final List<String> expectedResults = new ArrayList<>();
            expectedResults.add("[47, ABC]");
            expectedResults.add("[27, ABC]");
            expectedResults.add("[37, ABC]");
            expectedResults.add("[37, ABC]");
            expectedResults.add("[47, ABC]");
            expectedResults.add("[57, ABC]");

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        } finally {
            executor.closeSession();
        }
    }

    @Test
    void testBatchQueryExecutionMultipleTimes() throws Exception {
        final URL url = getClass().getClassLoader().getResource("test-data.csv");
        Objects.requireNonNull(url);
        final Map<String, String> replaceVars = new HashMap<>();
        replaceVars.put("$VAR_SOURCE_PATH1", url.getPath());

        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.BATCH.name());

        final Executor executor =
                createLocalExecutor(
                        Collections.singletonList(udfDependency), Configuration.fromMap(configMap));
        executor.openSession("test-session");
        initSession(executor, replaceVars);

        final List<String> expectedResults = new ArrayList<>();
        expectedResults.add("[47]");
        expectedResults.add("[27]");
        expectedResults.add("[37]");
        expectedResults.add("[37]");
        expectedResults.add("[47]");
        expectedResults.add("[57]");

        try {
            for (int i = 0; i < 3; i++) {
                final ResultDescriptor desc = executeQuery(executor, "SELECT * FROM TestView1");

                assertThat(desc.isMaterialized()).isTrue();

                final List<String> actualResults =
                        retrieveTableResult(desc.createResult(), desc.getRowDataStringConverter());

                TestBaseUtils.compareResultCollections(
                        expectedResults, actualResults, Comparator.naturalOrder());
            }
        } finally {
            executor.closeSession();
        }
    }

    @Test
    void testStopJob() throws Exception {
        final Map<String, String> configMap = new HashMap<>();
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.TABLE.name());
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.STREAMING.name());
        configMap.put(TableConfigOptions.TABLE_DML_SYNC.key(), "false");

        final Executor executor =
                createLocalExecutor(
                        Collections.singletonList(udfDependency), Configuration.fromMap(configMap));
        executor.openSession("test-session");

        final String srcDdl = "CREATE TABLE src (a STRING) WITH ('connector' = 'datagen')";
        final String snkDdl = "CREATE TABLE snk (a STRING) WITH ('connector' = 'blackhole')";
        final String insert = "INSERT INTO snk SELECT a FROM src;";

        try {
            executor.configureSession(srcDdl);
            executor.configureSession(snkDdl);
            ClientResult result = executor.executeStatement(insert);
            JobID jobID = result.getJobId();

            // wait till the job turns into running status or the test times out
            TestUtils.waitUntilAllTasksAreRunning(clusterClient, jobID);
            StringData savepointPath =
                    CollectionUtil.iteratorToList(
                                    executor.executeStatement(
                                            String.format("STOP JOB '%s' WITH SAVEPOINT", jobID)))
                            .get(0)
                            .getString(0);
            assertThat(
                            Files.exists(
                                    Paths.get(
                                            URI.create(
                                                    Preconditions.checkNotNull(savepointPath)
                                                            .toString()))))
                    .isTrue();
        } finally {
            executor.closeSession();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper method
    // --------------------------------------------------------------------------------------------

    private ResultDescriptor executeQuery(Executor executor, String query) {
        return new ResultDescriptor(executor.executeStatement(query), executor.getSessionConfig());
    }

    private Executor createLocalExecutor() {
        return createLocalExecutor(Collections.emptyList(), new Configuration());
    }

    private Executor createLocalExecutor(List<URL> dependencies, Configuration configuration) {
        configuration.addAll(clusterClient.getFlinkConfiguration());
        DefaultContext defaultContext =
                new DefaultContext(
                        dependencies,
                        configuration,
                        InetSocketAddress.createUnresolved(
                                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetAddress(),
                                SQL_GATEWAY_REST_ENDPOINT_EXTENSION.getTargetPort()));
        return new ExecutorImpl(defaultContext);
    }

    private void initSession(Executor executor, Map<String, String> replaceVars) {
        for (String sql : getInitSQL(replaceVars)) {
            executor.configureSession(sql);
        }
    }

    private void executeStreamQueryTable(
            Map<String, String> replaceVars,
            Map<String, String> configMap,
            String query,
            List<String> expectedResults)
            throws Exception {

        final Executor executor =
                createLocalExecutor(
                        Collections.singletonList(udfDependency), Configuration.fromMap(configMap));
        executor.openSession("test-session");
        initSession(executor, replaceVars);

        try {
            // start job and retrieval
            final ResultDescriptor desc = executeQuery(executor, query);

            assertThat(desc.isMaterialized()).isTrue();

            final List<String> actualResults =
                    retrieveTableResult(desc.createResult(), desc.getRowDataStringConverter());

            TestBaseUtils.compareResultCollections(
                    expectedResults, actualResults, Comparator.naturalOrder());
        } finally {
            executor.closeSession();
        }
    }

    private List<String> retrieveTableResult(
            MaterializedResult materializedResult,
            RowDataToStringConverter rowDataToStringConverter)
            throws InterruptedException {

        final List<String> actualResults = new ArrayList<>();
        while (true) {
            Thread.sleep(50); // slow the processing down
            final TypedResult<Integer> result = materializedResult.snapshot(2);
            if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                actualResults.clear();
                IntStream.rangeClosed(1, result.getPayload())
                        .forEach(
                                (page) -> {
                                    for (RowData row : materializedResult.retrievePage(page)) {
                                        actualResults.add(
                                                StringUtils.arrayAwareToString(
                                                        rowDataToStringConverter.convert(row)));
                                    }
                                });
            } else if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            }
        }

        return actualResults;
    }

    private List<String> retrieveChangelogResult(
            ChangelogCollectResult collectResult, RowDataToStringConverter rowDataToStringConverter)
            throws InterruptedException {

        final List<String> actualResults = new ArrayList<>();
        while (true) {
            Thread.sleep(50); // slow the processing down
            final TypedResult<List<RowData>> result = collectResult.retrieveChanges();
            if (result.getType() == TypedResult.ResultType.PAYLOAD) {
                for (RowData row : result.getPayload()) {
                    actualResults.add(
                            StringUtils.arrayAwareToString(rowDataToStringConverter.convert(row)));
                }
            } else if (result.getType() == TypedResult.ResultType.EOS) {
                break;
            }
        }
        return actualResults;
    }

    private Map<String, String> getDefaultSessionConfigMap() {
        HashMap<String, String> configMap = new HashMap<>();
        configMap.put(RUNTIME_MODE.key(), RuntimeExecutionMode.STREAMING.name());
        configMap.put(EXECUTION_RESULT_MODE.key(), ResultMode.CHANGELOG.name());
        configMap.put(EXECUTION_MAX_TABLE_RESULT_ROWS.key(), "100");
        return configMap;
    }

    private List<String> getInitSQL(final Map<String, String> replaceVars) {
        return Stream.of(
                        String.format(
                                "CREATE FUNCTION scalarUDF AS '%s'",
                                UserDefinedFunctions.ScalarUDF.class.getName()),
                        String.format(
                                "CREATE FUNCTION aggregateUDF AS '%s'",
                                AggregateFunction.class.getName()),
                        String.format(
                                "CREATE FUNCTION tableUDF AS '%s'",
                                UserDefinedFunctions.TableUDF.class.getName()),
                        "CREATE TABLE TableNumber1 (\n"
                                + "  IntegerField1 INT,\n"
                                + "  StringField1 STRING,\n"
                                + "  TimestampField1 TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '$VAR_SOURCE_PATH1',\n"
                                + "  'format' = 'csv',\n"
                                + "  'csv.ignore-parse-errors' = 'true',\n"
                                + "  'csv.allow-comments' = 'true'\n"
                                + ")\n",
                        "CREATE VIEW TestView1 AS SELECT scalarUDF(IntegerField1, 5) FROM TableNumber1\n",
                        "CREATE TABLE TableSourceSink (\n"
                                + "  BooleanField BOOLEAN,\n"
                                + "  StringField2 STRING,\n"
                                + "  TimestampField2 TIMESTAMP\n"
                                + ") WITH (\n"
                                + "  'connector' = 'filesystem',\n"
                                + "  'path' = '$VAR_SOURCE_SINK_PATH',\n"
                                + "  'format' = 'csv',\n"
                                + "  'csv.ignore-parse-errors' = 'true',\n"
                                + "  'csv.allow-comments' = 'true'\n"
                                + ")\n",
                        "CREATE VIEW TestView2 AS SELECT * FROM TestView1\n")
                .map(
                        sql -> {
                            for (Map.Entry<String, String> replaceVar : replaceVars.entrySet()) {
                                sql = sql.replace(replaceVar.getKey(), replaceVar.getValue());
                            }
                            return sql;
                        })
                .collect(Collectors.toList());
    }

    // --------------------------------------------------------------------------------------------
    // Test functions
    // --------------------------------------------------------------------------------------------

    /** Scala Function for test. */
    public static class TestScalaFunction extends ScalarFunction {
        public long eval(int i, long l, String s) {
            return i + l + s.length();
        }
    }
}
