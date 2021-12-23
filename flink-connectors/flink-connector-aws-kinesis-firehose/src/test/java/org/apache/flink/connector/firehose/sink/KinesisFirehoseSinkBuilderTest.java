/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.services.firehose.model.Record;

/** Covers construction, defaults and sanity checking of {@link KinesisFirehoseSinkBuilder}. */
public class KinesisFirehoseSinkBuilderTest {
    private static final ElementConverter<String, Record> ELEMENT_CONVERTER_PLACEHOLDER =
            KinesisFirehoseSinkElementConverter.<String>builder()
                    .setSerializationSchema(new SimpleStringSchema())
                    .build();

    @Test
    public void elementConverterOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                KinesisFirehoseSink.builder()
                                        .setDeliveryStreamName("deliveryStream")
                                        .build())
                .withMessageContaining(
                        "ElementConverter must be not null when initializing the AsyncSinkBase.");
    }

    @Test
    public void streamNameOfSinkMustBeSetWhenBuilt() {
        Assertions.assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                KinesisFirehoseSink.<String>builder()
                                        .setElementConverter(ELEMENT_CONVERTER_PLACEHOLDER)
                                        .build())
                .withMessageContaining(
                        "The delivery stream name must not be null when initializing the KDF Sink.");
    }

    @Test
    public void streamNameOfSinkMustBeSetToNonEmptyWhenBuilt() {
        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                KinesisFirehoseSink.<String>builder()
                                        .setDeliveryStreamName("")
                                        .setElementConverter(ELEMENT_CONVERTER_PLACEHOLDER)
                                        .build())
                .withMessageContaining(
                        "The delivery stream name must be set when initializing the KDF Sink.");
    }
}
