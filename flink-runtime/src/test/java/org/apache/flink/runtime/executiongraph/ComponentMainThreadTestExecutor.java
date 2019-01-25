/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.rules.ExternalResource;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Util to run test calls with a provided main thread executor.
 */
public class ComponentMainThreadTestExecutor {

	/** The main thread executor to which execution is delegated. */
	@Nonnull
	private final TestComponentMainThreadExecutor mainThreadExecutor;

	public ComponentMainThreadTestExecutor(@Nonnull TestComponentMainThreadExecutor mainThreadExecutor) {
		this.mainThreadExecutor = mainThreadExecutor;
	}

	/**
	 * Executes the given supplier with the main thread executor until completion, returns the result or a exception.
	 * This method blocks until the execution is complete.
	 */
	public <U> U execute(@Nonnull SupplierWithException<U, Throwable> supplierWithException) {
		return CompletableFuture.supplyAsync(
			FunctionUtils.uncheckedSupplier(supplierWithException),
			mainThreadExecutor)
			.join();
	}

	/**
	 * Executes the given runnable with the main thread executor and blocks until completion.
	 */
	public void execute(@Nonnull ThrowingRunnable<Throwable> throwingRunnable) {
		execute(() -> {
			throwingRunnable.run();
			return null;
		});
	}

	@Nonnull
	public TestComponentMainThreadExecutor getMainThreadExecutor() {
		return mainThreadExecutor;
	}

	/**
	 * Test resource for convenience.
	 */
	public static class Resource extends ExternalResource {

		private ComponentMainThreadTestExecutor componentMainThreadTestExecutor;
		private ScheduledExecutorService innerExecutorService;

		@Override
		protected void before() throws Throwable {
			this.innerExecutorService = Executors.newSingleThreadScheduledExecutor();
			this.componentMainThreadTestExecutor =
				new ComponentMainThreadTestExecutor(
					TestComponentMainThreadExecutor.forSingleThreadExecutor(innerExecutorService));
		}

		@Override
		protected void after() {
			this.innerExecutorService.shutdown();
			this.innerExecutorService.shutdownNow();
			try {
				this.innerExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		public ComponentMainThreadTestExecutor getComponentMainThreadTestExecutor() {
			return componentMainThreadTestExecutor;
		}
	}
}
