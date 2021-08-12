/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.connectors.dynamodb.retry;

import org.apache.flink.streaming.connectors.dynamodb.batch.BatchWriterAttemptResult;

/** Retry policy for a batch writer. */
public interface BatchWriterRetryPolicy {

    boolean shouldRetry(BatchWriterAttemptResult attemptResult);

    int getBackOffTime(BatchWriterAttemptResult attemptResult);

    /**
     * Identify non-retryable exceptions while handling DynamoDB write. If exception identified as
     * non-retryable the write attempt will be marked as unsuccessful and can be handled later.
     */
    boolean isNotRetryableException(Exception e);

    /**
     * Identify throttling DynamoDB write. If exception identified as throttling exception, backoff
     * will be applied and the request will be retried after the backoff time.
     */
    boolean isThrottlingException(Exception e);
}
