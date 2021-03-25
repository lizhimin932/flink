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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.context.ExecutionContext;
import org.apache.flink.table.client.gateway.context.SessionContext;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executor that performs the Flink communication locally. The calls are blocking depending on the
 * response time to the Flink cluster. Flink jobs are not blocking.
 */
public class LocalExecutor implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(LocalExecutor.class);

    // Map to hold all the available sessions. the key is session identifier, and the value is the
    // SessionContext
    private final ConcurrentHashMap<String, SessionContext> contextMap;

    private final DefaultContext defaultContext;

    /** Creates a local executor for submitting table programs and retrieving results. */
    public LocalExecutor(DefaultContext defaultContext) {
        this.contextMap = new ConcurrentHashMap<>();
        this.defaultContext = defaultContext;
    }

    @Override
    public void start() {
        // nothing to do yet
    }

    @Override
    public String openSession(@Nullable String sessionId) throws SqlExecutionException {
        SessionContext sessionContext =
                LocalContextUtils.buildSessionContext(sessionId, defaultContext);
        sessionId = sessionContext.getSessionId();
        if (this.contextMap.containsKey(sessionId)) {
            throw new SqlExecutionException(
                    "Found another session with the same session identifier: " + sessionId);
        } else {
            this.contextMap.put(sessionId, sessionContext);
        }
        return sessionId;
    }

    @Override
    public void closeSession(String sessionId) throws SqlExecutionException {
        // Remove the session's ExecutionContext from contextMap and close it.
        SessionContext context = this.contextMap.remove(sessionId);
        if (context != null) {
            context.close();
        }
    }

    private SessionContext getSessionContext(String sessionId) {
        SessionContext context = this.contextMap.get(sessionId);
        if (context == null) {
            throw new SqlExecutionException("Invalid session identifier: " + sessionId);
        }
        return context;
    }

    /**
     * Get the existed {@link ExecutionContext} from contextMap, or thrown exception if does not
     * exist.
     */
    @VisibleForTesting
    protected ExecutionContext getExecutionContext(String sessionId) throws SqlExecutionException {
        return getSessionContext(sessionId).getExecutionContext();
    }

    @Override
    public Map<String, String> getSessionProperties(String sessionId) throws SqlExecutionException {
        return getSessionContext(sessionId).getConfigMap();
    }

    @Override
    public ReadableConfig getSessionConfig(String sessionId) throws SqlExecutionException {
        return getSessionContext(sessionId).getReadableConfig();
    }

    @Override
    public void resetSessionProperties(String sessionId) throws SqlExecutionException {
        SessionContext context = getSessionContext(sessionId);
        context.reset();
    }

    @Override
    public void resetSessionProperty(String sessionId, String key) throws SqlExecutionException {
        SessionContext context = getSessionContext(sessionId);
        context.reset(key);
    }

    @Override
    public void setSessionProperty(String sessionId, String key, String value)
            throws SqlExecutionException {
        SessionContext context = getSessionContext(sessionId);
        context.set(key, value);
    }

    @Override
    public TableResult executeOperation(String sessionId, Operation operation)
            throws SqlExecutionException {
        final ExecutionContext context = getExecutionContext(sessionId);
        final TableEnvironmentInternal tEnv =
                (TableEnvironmentInternal) context.getTableEnvironment();
        try {
            return context.wrapClassLoader(() -> tEnv.executeInternal(operation));
        } catch (Exception e) {
            throw new SqlExecutionException("Could not execute operation: " + operation, e);
        }
    }

    @Override
    public Optional<Operation> parseStatement(String sessionId, String statement)
            throws SqlExecutionException {
        final ExecutionContext context = getExecutionContext(sessionId);
        final TableEnvironment tableEnv = context.getTableEnvironment();
        Parser parser = ((TableEnvironmentInternal) tableEnv).getParser();

        List<Operation> operations;
        try {
            operations = context.wrapClassLoader(() -> parser.parse(statement));
        } catch (Exception e) {
            throw new SqlExecutionException("Failed to parse statement: " + statement, e);
        }
        if (operations.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(operations.get(0));
        }
    }

    @Override
    public List<String> completeStatement(String sessionId, String statement, int position) {
        final ExecutionContext context = getExecutionContext(sessionId);
        final TableEnvironmentInternal tableEnv =
                (TableEnvironmentInternal) context.getTableEnvironment();

        try {
            return context.wrapClassLoader(
                    () ->
                            Arrays.asList(
                                    tableEnv.getParser().getCompletionHints(statement, position)));
        } catch (Throwable t) {
            // catch everything such that the query does not crash the executor
            if (LOG.isDebugEnabled()) {
                LOG.debug("Could not complete statement at " + position + ":" + statement, t);
            }
            return Collections.emptyList();
        }
    }
}
