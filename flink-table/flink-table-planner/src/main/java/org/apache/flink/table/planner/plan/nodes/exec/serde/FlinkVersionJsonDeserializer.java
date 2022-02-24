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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.FlinkVersion;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/** JSON deserializer for {@link FlinkVersion}. */
public class FlinkVersionJsonDeserializer extends StdDeserializer<FlinkVersion> {
    private static final long serialVersionUID = 1L;

    public FlinkVersionJsonDeserializer() {
        super(FlinkVersion.class);
    }

    @Override
    public FlinkVersion deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        String codeString = jsonParser.getValueAsString();
        return FlinkVersion.byCode(codeString)
                .orElseThrow(
                        () ->
                                new ValidationException(
                                        "Unknown Flink version '" + codeString + "'"));
    }
}
