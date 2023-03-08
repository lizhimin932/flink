/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.gateway.service.operation;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.operation.OperationStatus;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.results.ResultSetImpl;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.table.gateway.api.utils.ThreadUtils;
import org.apache.flink.table.gateway.service.utils.IgnoreExceptionHandler;
import org.apache.flink.table.gateway.service.utils.SqlCancelException;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;
import static org.apache.flink.table.gateway.api.results.ResultSet.ResultType.PAYLOAD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link OperationManager}. */
public class OperationManagerTest {

    private static final ExecutorService EXECUTOR_SERVICE =
            ThreadUtils.newThreadPool(5, 500, 60_0000, "operation-manager-test");

    private static OperationManager operationManager;
    private static ResultSet defaultResultSet;

    private final ThreadFactory threadFactory =
            new ExecutorThreadFactory(
                    "SqlGatewayService Test Pool", IgnoreExceptionHandler.INSTANCE);

    @BeforeEach
    public void setUp() {
        operationManager = new OperationManager(EXECUTOR_SERVICE);
        defaultResultSet =
                new ResultSetImpl(
                        PAYLOAD,
                        1L,
                        ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT())),
                        Collections.singletonList(GenericRowData.of(1L)),
                        SIMPLE_ROW_DATA_TO_STRING_CONVERTER,
                        false,
                        null,
                        ResultKind.SUCCESS_WITH_CONTENT);
    }

    @AfterEach
    public void cleanEach() {
        operationManager.close();
    }

    @AfterAll
    public static void cleanUp() {
        EXECUTOR_SERVICE.shutdown();
    }

    @Test
    public void testRunOperationAsynchronously() throws Exception {
        OperationHandle operationHandle = operationManager.submitOperation(() -> defaultResultSet);

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isNotEqualTo(OperationStatus.ERROR);

        assertThat(operationManager.getOperationResultSchema(operationHandle))
                .isEqualTo(ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT())));

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.FINISHED);
    }

    @Test
    public void testRunOperationSynchronously() throws Exception {
        OperationHandle operationHandle = operationManager.submitOperation(() -> defaultResultSet);
        operationManager.awaitOperationTermination(operationHandle);

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.FINISHED);

        assertThat(operationManager.fetchResults(operationHandle, 0, Integer.MAX_VALUE))
                .isEqualTo(defaultResultSet);
    }

    @Test
    public void testCancelOperation() throws Exception {
        CountDownLatch endRunningLatch = new CountDownLatch(1);
        OperationHandle operationHandle =
                operationManager.submitOperation(
                        () -> {
                            endRunningLatch.await();
                            return defaultResultSet;
                        });

        threadFactory.newThread(() -> operationManager.cancelOperation(operationHandle)).start();
        operationManager.awaitOperationTermination(operationHandle);

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.CANCELED);
    }

    @Test
    public void testCancelUninterruptedOperation() throws Exception {
        AtomicReference<Boolean> isRunning = new AtomicReference<>(false);
        OperationHandle operationHandle =
                operationManager.submitOperation(
                        () -> {
                            // mock cpu busy task that doesn't interrupt system call
                            while (true) {
                                isRunning.compareAndSet(false, true);
                            }
                        });
        CommonTestUtils.waitUtil(
                isRunning::get, Duration.ofSeconds(10), "Failed to start up the task.");
        assertThatThrownBy(() -> operationManager.cancelOperation(operationHandle))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                SqlCancelException.class,
                                String.format(
                                        "Operation '%s' did not react to \"Future.cancel(true)\" and "
                                                + "is stuck for %s seconds in method.\n",
                                        operationHandle, 5)));

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.CANCELED);
    }

    @Test
    public void testCloseUninterruptedOperation() throws Exception {
        AtomicReference<Boolean> isRunning = new AtomicReference<>(false);
        for (int i = 0; i < 10; i++) {
            threadFactory
                    .newThread(
                            () -> {
                                operationManager.submitOperation(
                                        () -> {
                                            // mock cpu busy task that doesn't interrupt system call
                                            while (true) {
                                                isRunning.compareAndSet(false, true);
                                            }
                                        });
                            })
                    .start();
        }
        CommonTestUtils.waitUtil(
                isRunning::get, Duration.ofSeconds(10), "Failed to start up the task.");

        assertThatThrownBy(() -> operationManager.close())
                .satisfies(FlinkAssertions.anyCauseMatches(SqlCancelException.class));
        assertThat(operationManager.getOperationCount()).isEqualTo(0);
    }

    @Test
    public void testCloseOperation() throws Exception {
        CountDownLatch endRunningLatch = new CountDownLatch(1);
        OperationHandle operationHandle =
                operationManager.submitOperation(
                        () -> {
                            endRunningLatch.await();
                            return defaultResultSet;
                        });

        threadFactory.newThread(() -> operationManager.closeOperation(operationHandle)).start();
        operationManager.awaitOperationTermination(operationHandle);

        assertThatThrownBy(() -> operationManager.getOperation(operationHandle))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                SqlGatewayException.class,
                                String.format(
                                        "Can not find the submitted operation in the OperationManager with the %s.",
                                        operationHandle)));
    }

    @Test
    public void testRunOperationSynchronouslyWithError() {
        OperationHandle operationHandle =
                operationManager.submitOperation(
                        () -> {
                            throw new SqlExecutionException("Execution error.");
                        });

        assertThatThrownBy(() -> operationManager.awaitOperationTermination(operationHandle))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                SqlExecutionException.class, "Execution error."));

        assertThat(operationManager.getOperationInfo(operationHandle).getStatus())
                .isEqualTo(OperationStatus.ERROR);
    }
}
