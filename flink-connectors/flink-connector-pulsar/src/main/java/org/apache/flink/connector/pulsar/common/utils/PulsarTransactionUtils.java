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

package org.apache.flink.connector.pulsar.common.utils;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;

/**
 * A suit of workarounds for the Pulsar Transaction. We couldn't create the {@link Transaction}
 * directly from {@link PulsarClient#newTransaction()}, because it would add the transaction into a
 * netty timer which couldn't be GC before the transaction timeout. Since we have a proper
 * management on flink for the transaction, we have to fix this issue before Pulsar release a new
 * version.
 */
public final class PulsarTransactionUtils {

    private PulsarTransactionUtils() {
        // No public constructor
    }

    public static Transaction createTransaction(PulsarClient pulsarClient, long timeoutMs) {

        try {
            CompletableFuture<Transaction> future =
                    sneakyClient(pulsarClient::newTransaction)
                            .withTransactionTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                            .build();

            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    /**
     * This is a bug in original {@link TransactionCoordinatorClientException#unwrap(Throwable)}
     * method. Pulsar wraps the {@link ExecutionException} which hides the read execution exception.
     */
    public static TransactionCoordinatorClientException unwrap(
            TransactionCoordinatorClientException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ExecutionException) {
            Throwable throwable = cause.getCause();
            if (throwable instanceof TransactionCoordinatorClientException) {
                return (TransactionCoordinatorClientException) throwable;
            }
        }
        return e;
    }
}
