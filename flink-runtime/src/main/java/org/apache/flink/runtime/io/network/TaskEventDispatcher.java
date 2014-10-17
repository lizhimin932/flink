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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.AbstractID;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.runtime.util.event.EventListener;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The task event handler dispatches events flowing backwards from a receiver
 * to a producer. It only supports programs, where the producer and consumer
 * are pipelined and running at the same time.
 * <p/>
 * The publish method is either called from the local input channel or the
 * network I/O thread.
 * <p/>
 * This class is thread-safe.
 */
class TaskEventDispatcher {

	ConcurrentMap<AbstractID, EventListener<TaskEvent>> listeners = new ConcurrentHashMap<AbstractID, EventListener<TaskEvent>>();

	public void register(AbstractID id, EventListener<TaskEvent> listener) {
		if (listeners.putIfAbsent(id, listener) != null) {
			throw new IllegalStateException("Event Dispatcher already contained event listener.");
		}
	}

	public void unregister(AbstractID id) {
		listeners.remove(id);
	}

	/**
	 * Publishes the event to the registered {@link EventListener} instance.
	 */
	public void publish(AbstractID id, TaskEvent event) {
		EventListener<TaskEvent> listener = listeners.get(id);

		if (listener != null) {
			listener.onEvent(event);
		}
	}
}
