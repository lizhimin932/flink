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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

/** A {@link DynamicTableSinkFactory} for discovering {@link ElasticsearchDynamicSink}. */
@Internal
public class Elasticsearch7DynamicSinkFactory extends ElasticsearchDynamicSinkFactoryBase {
    private static final String FACTORY_IDENTIFIER = "elasticsearch-7";

    public Elasticsearch7DynamicSinkFactory() {
        super(FACTORY_IDENTIFIER, Elasticsearch7SinkBuilder::new);
    }
}
