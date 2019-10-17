/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.CompletableFuture;

/**
 * Runner responsible for executing a {@link Dispatcher} or a subclass thereof.
 */
class DispatcherRunnerImpl implements DispatcherRunner {

	private final Dispatcher dispatcher;

	DispatcherRunnerImpl(
		DispatcherFactory dispatcherFactory,
		RpcService rpcService,
		PartialDispatcherServices partialDispatcherServices) throws Exception {
		this.dispatcher = dispatcherFactory.createDispatcher(
			rpcService,
			partialDispatcherServices);

		dispatcher.start();
	}

	@Override
	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return dispatcher.closeAsync();
	}

	@Override
	public CompletableFuture<Tuple2<ApplicationStatus, String>> getShutDownFuture() {
		return dispatcher.getShutDownFuture();
	}
}
