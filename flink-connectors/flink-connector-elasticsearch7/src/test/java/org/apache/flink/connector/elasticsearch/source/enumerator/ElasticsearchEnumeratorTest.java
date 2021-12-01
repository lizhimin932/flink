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

package org.apache.flink.connector.elasticsearch.source.enumerator;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.ElasticsearchSourceConfiguration;
import org.apache.flink.connector.elasticsearch.source.split.ElasticsearchSplit;

import org.apache.http.HttpHost;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;

/** Tests for {@link ElasticsearchEnumerator}. */
public class ElasticsearchEnumeratorTest {
    private static final int NUM_SUBTASKS = 3;

    @Test
    public void testStartWithDiscoverSplitsOnce() throws Exception {
        try (MockSplitEnumeratorContext<ElasticsearchSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                ElasticsearchEnumerator enumerator = createEnumerator(context)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // splits.
            enumerator.start();
            Assertions.assertThat(context.getPeriodicCallables()).isEmpty();

            Assertions.assertThat(context.getOneTimeCallables())
                    .withFailMessage(
                            "A one time split discovery callable should have been scheduled")
                    .size()
                    .isEqualTo(1);
        }
    }

    // ----------------------------------------

    private ElasticsearchEnumerator createEnumerator(
            MockSplitEnumeratorContext<ElasticsearchSplit> enumContext) {
        NetworkClientConfig networkClientConfig =
                new NetworkClientConfig(null, null, null, null, null, null);

        ElasticsearchSourceConfiguration sourceConfiguration =
                new ElasticsearchSourceConfiguration(
                        Collections.singletonList(new HttpHost("127.0.0.1", 9200, "http")),
                        "my-index",
                        3,
                        Duration.ofMinutes(5));

        return new ElasticsearchEnumerator(sourceConfiguration, networkClientConfig, enumContext);
    }
}
