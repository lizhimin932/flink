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

package org.apache.flink.table.gateway.service.result;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.utils.SqlGatewayException;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;

import org.apache.commons.collections.iterators.IteratorChain;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for {@link ResultFetcher}. */
public class ResultFetcherTest extends TestLogger {

    private static ResolvedSchema schema;
    private static List<RowData> data;

    @BeforeAll
    public static void setUp() {
        schema =
                ResolvedSchema.of(
                        Column.physical("boolean", DataTypes.BOOLEAN()),
                        Column.physical("int", DataTypes.INT()),
                        Column.physical("bigint", DataTypes.BIGINT()),
                        Column.physical("varchar", DataTypes.STRING()),
                        Column.physical("decimal(10, 5)", DataTypes.DECIMAL(10, 5)),
                        Column.physical(
                                "timestamp", DataTypes.TIMESTAMP(6).bridgedTo(Timestamp.class)),
                        Column.physical("binary", DataTypes.BYTES()));
        data =
                Arrays.asList(
                        GenericRowData.ofKind(
                                RowKind.INSERT,
                                null,
                                1,
                                2L,
                                "abc",
                                BigDecimal.valueOf(1.23),
                                Timestamp.valueOf("2020-03-01 18:39:14"),
                                new byte[] {50, 51, 52, -123, 54, 93, 115, 126}),
                        GenericRowData.ofKind(
                                RowKind.UPDATE_BEFORE,
                                false,
                                null,
                                0L,
                                "",
                                BigDecimal.valueOf(1),
                                Timestamp.valueOf("2020-03-01 18:39:14.1"),
                                new byte[] {100, -98, 32, 121, -125}),
                        GenericRowData.ofKind(
                                RowKind.UPDATE_AFTER,
                                true,
                                Integer.MAX_VALUE,
                                null,
                                "abcdefg",
                                BigDecimal.valueOf(12345),
                                Timestamp.valueOf("2020-03-01 18:39:14.12"),
                                new byte[] {-110, -23, 1, 2}),
                        GenericRowData.ofKind(
                                RowKind.DELETE,
                                false,
                                Integer.MIN_VALUE,
                                Long.MAX_VALUE,
                                null,
                                BigDecimal.valueOf(12345.06789),
                                Timestamp.valueOf("2020-03-01 18:39:14.123"),
                                new byte[] {50, 51, 52, -123, 54, 93, 115, 126}),
                        GenericRowData.ofKind(
                                RowKind.INSERT,
                                true,
                                100,
                                Long.MIN_VALUE,
                                "abcdefg111",
                                null,
                                Timestamp.valueOf("2020-03-01 18:39:14.123456"),
                                new byte[] {110, 23, -1, -2}),
                        GenericRowData.ofKind(
                                RowKind.DELETE,
                                null,
                                -1,
                                -1L,
                                "abcdefghijklmnopqrstuvwxyz",
                                BigDecimal.valueOf(-12345.06789),
                                null,
                                null),
                        GenericRowData.ofKind(
                                RowKind.INSERT,
                                null,
                                -1,
                                -1L,
                                "这是一段中文",
                                BigDecimal.valueOf(-12345.06789),
                                Timestamp.valueOf("2020-03-04 18:39:14"),
                                new byte[] {-3, -2, -1, 0, 1, 2, 3}),
                        GenericRowData.ofKind(
                                RowKind.DELETE,
                                null,
                                -1,
                                -1L,
                                "これは日本語をテストするための文です",
                                BigDecimal.valueOf(-12345.06789),
                                Timestamp.valueOf("2020-03-04 18:39:14"),
                                new byte[] {-3, -2, -1, 0, 1, 2, 3}));
    }

    @Test
    public void testFetchResultsMultipleTimesWithLimitedBufferSize() {
        int bufferSize = data.size() / 2;
        ResultFetcher fetcher =
                buildResultFetcher(Collections.singletonList(data.iterator()), bufferSize);

        runFetchMultipleTimes(fetcher, bufferSize, data.size());
    }

    @Test
    public void testFetchResultsMultipleTimesWithLimitedFetchSize() {
        int bufferSize = data.size();
        ResultFetcher fetcher =
                buildResultFetcher(Collections.singletonList(data.iterator()), bufferSize);

        runFetchMultipleTimes(fetcher, bufferSize, data.size() / 2);
    }

    @Test
    public void testFetchResultInParallel() throws Exception {
        int bufferSize = data.size() / 2;
        ResultFetcher fetcher =
                buildResultFetcher(Collections.singletonList(data.iterator()), bufferSize);

        AtomicReference<Boolean> isEqual = new AtomicReference<>(true);
        int fetchThreadNum = 100;
        CountDownLatch latch = new CountDownLatch(fetchThreadNum);

        CommonTestUtils.waitUtil(
                () -> fetcher.getResultStore().getBufferedRecordSize() > 0,
                Duration.ofSeconds(10),
                "Failed to wait the buffer has data.");
        List<RowData> firstFetch = fetcher.fetchResults(0, Integer.MAX_VALUE).getData();
        for (int i = 0; i < fetchThreadNum; i++) {
            new Thread(
                            () -> {
                                ResultSet resultSet = fetcher.fetchResults(0, Integer.MAX_VALUE);

                                if (!firstFetch.equals(resultSet.getData())) {
                                    isEqual.set(false);
                                }
                                latch.countDown();
                            })
                    .start();
        }

        latch.await();
        assertEquals(true, isEqual.get());
    }

    @Test
    public void testFetchResultAfterClose() throws Exception {
        ResultFetcher fetcher =
                buildResultFetcher(Collections.singletonList(data.iterator()), data.size() + 1);
        List<RowData> actual = Collections.emptyList();
        long token = 0L;

        while (actual.size() < 1) {
            // fill the fetcher buffer
            ResultSet resultSet = fetcher.fetchResults(token, 1);
            token = checkNotNull(resultSet.getNextToken());
            actual = resultSet.getData();
        }
        assertEquals(data.subList(0, 1), actual);
        fetcher.close();

        long testToken = token;
        AtomicReference<Boolean> meetEnd = new AtomicReference<>(false);
        new Thread(
                        () -> {
                            // Should meet EOS in the end.
                            long nextToken = testToken;
                            while (true) {
                                ResultSet resultSet =
                                        fetcher.fetchResults(nextToken, Integer.MAX_VALUE);
                                if (resultSet.getResultType() == ResultSet.ResultType.EOS) {
                                    break;
                                }
                                nextToken = checkNotNull(resultSet.getNextToken());
                            }
                            meetEnd.set(true);
                        })
                .start();

        CommonTestUtils.waitUtil(
                meetEnd::get,
                Duration.ofSeconds(10),
                "Should get EOS when fetch results from the closed fetcher.");
    }

    @Test
    public void testFetchResultWithToken() {
        ResultFetcher fetcher =
                buildResultFetcher(Collections.singletonList(data.iterator()), data.size());
        Long nextToken = 0L;
        List<RowData> actual = new ArrayList<>();
        ResultSet resultSetBefore = null;
        while (nextToken != null) {
            if (resultSetBefore != null) {
                assertEquals(resultSetBefore, fetcher.fetchResults(nextToken - 1, data.size()));
            }

            ResultSet resultSet = fetcher.fetchResults(nextToken, data.size());
            ResultSet resultSetWithSameToken = fetcher.fetchResults(nextToken, data.size());

            assertEquals(resultSet, resultSetWithSameToken);
            if (resultSet.getResultType() == ResultSet.ResultType.EOS) {
                break;
            }
            resultSetBefore = resultSet;

            actual.addAll(checkNotNull(resultSet.getData()));
            nextToken = resultSet.getNextToken();
        }

        assertEquals(data, actual);
    }

    @Test
    public void testFetchFailedResult() {
        String message = "Artificial Exception";
        ResultFetcher fetcher =
                buildResultFetcher(
                        Arrays.asList(new ErrorIterator(message), data.iterator()), data.size());

        assertThatThrownBy(
                        () -> {
                            Long token = 0L;
                            while (token != null) {
                                // Use loop to fetch results from the ErrorIterator
                                token =
                                        fetcher.fetchResults(token, Integer.MAX_VALUE)
                                                .getNextToken();
                            }
                        })
                .satisfies(FlinkAssertions.anyCauseMatches(message));
    }

    @Test
    public void testFetchIllegalToken() {
        ResultFetcher fetcher =
                buildResultFetcher(Collections.singletonList(data.iterator()), data.size());
        assertThatThrownBy(() -> fetcher.fetchResults(2, Integer.MAX_VALUE))
                .satisfies(FlinkAssertions.anyCauseMatches("Expecting token to be 0, but found 2"));
    }

    @Test
    public void testFetchBeforeWithDifferentSize() throws Exception {
        ResultFetcher fetcher =
                buildResultFetcher(Collections.singletonList(data.iterator()), data.size() / 2);
        CommonTestUtils.waitUtil(
                () -> fetcher.getResultStore().getBufferedRecordSize() > 1,
                Duration.ofSeconds(10),
                "Failed to make cached records num larger than 1.");

        ResultSet firstFetch = fetcher.fetchResults(0, Integer.MAX_VALUE);
        int firstFetchSize = firstFetch.getData().size();
        assertThatThrownBy(() -> fetcher.fetchResults(0, firstFetchSize - 1))
                .satisfies(
                        FlinkAssertions.anyCauseMatches(
                                String.format(
                                        "As the same token is provided, fetch size must be not less than the previous returned buffer size."
                                                + " Previous returned result size is %s, current max_fetch_size to be %s.",
                                        firstFetch.getData().size(), firstFetchSize - 1)));
    }

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private ResultFetcher buildResultFetcher(List<Iterator<RowData>> rows, int bufferSize) {
        OperationHandle operationHandle = OperationHandle.create();
        return new ResultFetcher(
                operationHandle,
                schema,
                CloseableIterator.adapterForIterator(new IteratorChain(rows)),
                bufferSize);
    }

    private void runFetchMultipleTimes(ResultFetcher fetcher, int bufferSize, int fetchSize) {
        List<RowData> fetchedRows = new ArrayList<>();
        ResultSet currentResult = null;
        Long token = 0L;

        while (token != null) {
            currentResult = fetcher.fetchResults(token, fetchSize);
            assertTrue(
                    checkNotNull(currentResult.getData()).size()
                            <= Math.min(bufferSize, fetchSize));
            token = currentResult.getNextToken();
            fetchedRows.addAll(currentResult.getData());
        }

        assertEquals(ResultSet.ResultType.EOS, checkNotNull(currentResult).getResultType());
        assertEquals(data, fetchedRows);
    }

    // --------------------------------------------------------------------------------------------

    private static class ErrorIterator implements Iterator<RowData> {

        private final String errorMsg;

        public ErrorIterator(String errorMsg) {
            this.errorMsg = errorMsg;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public RowData next() {
            throw new SqlGatewayException(errorMsg);
        }
    }
}
