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

package org.apache.flink.runtime.rest.messages.job.savepoints.stop;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/** {@link RequestBody} for stopping a job with a savepoint. */
public class StopWithSavepointRequestBody implements RequestBody {

    public static final String FIELD_NAME_TARGET_DIRECTORY = "targetDirectory";

    public static final String FIELD_NAME_SAVEPOINT_TIMEOUT = "savepointTimeout";

    private static final String FIELD_NAME_DRAIN = "drain";

    @JsonProperty(FIELD_NAME_TARGET_DIRECTORY)
    @Nullable
    private final String targetDirectory;

    @JsonProperty(FIELD_NAME_SAVEPOINT_TIMEOUT)
    private final long savepointTimeout;

    @JsonProperty(FIELD_NAME_DRAIN)
    private final boolean drain;

    public StopWithSavepointRequestBody(final String targetDirectory, final boolean drain) {
        this(targetDirectory, null, drain);
    }

    @JsonCreator
    public StopWithSavepointRequestBody(
            @Nullable @JsonProperty(FIELD_NAME_TARGET_DIRECTORY) final String targetDirectory,
            @Nullable @JsonProperty(FIELD_NAME_SAVEPOINT_TIMEOUT) final Long savepointTimeout,
            @Nullable @JsonProperty(FIELD_NAME_DRAIN) final Boolean drain) {
        this.targetDirectory = targetDirectory;
        this.savepointTimeout = savepointTimeout != null ? savepointTimeout : 0;
        this.drain = drain != null ? drain : false;
    }

    @Nullable
    public String getTargetDirectory() {
        return targetDirectory;
    }

    public long getSavepointTimeout() {
        return savepointTimeout;
    }

    public boolean shouldDrain() {
        return drain;
    }
}
