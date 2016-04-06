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

package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * CassandraSinkBase is the common abstract class of {@link CassandraPojoSink} and {@link CassandraTupleSink}.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class CassandraSinkBase<IN, V> extends RichSinkFunction<IN> {
	protected static final Logger LOG = LoggerFactory.getLogger(CassandraSinkBase.class);
	protected transient Cluster cluster;
	protected transient Session session;

	protected transient Throwable exception = null;
	protected transient FutureCallback<V> callback;

	private final ClusterBuilder builder;

	protected CassandraSinkBase(ClusterBuilder builder) {
		this.builder = builder;
		ClosureCleaner.clean(builder, true);
	}

	@Override
	public void open(Configuration configuration) {
		this.callback = new FutureCallback<V>() {
			@Override
			public void onSuccess(V ignored) {
			}

			@Override
			public void onFailure(Throwable t) {
				exception = t;
				LOG.error("Error while sending value.", t);
			}
		};
		this.cluster = builder.getCluster();
		this.session = cluster.connect();
	}

	@Override
	public void invoke(IN value) throws Exception {
		if (exception != null) {
			throw new IOException("invoke() failed", exception);
		}
		ListenableFuture<V> result = send(value);
		Futures.addCallback(result, callback);
	}

	public abstract ListenableFuture<V> send(IN value);

	@Override
	public void close() {
		try {
			session.close();
		} catch (Exception e) {
			LOG.error("Error while closing session.", e);
		}
		try {
			cluster.close();
		} catch (Exception e) {
			LOG.error("Error while closing cluster.", e);
		}
	}
}
