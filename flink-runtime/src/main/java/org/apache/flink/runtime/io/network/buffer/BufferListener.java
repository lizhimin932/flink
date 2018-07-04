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

package org.apache.flink.runtime.io.network.buffer;

/**
 * Interface of the availability of buffers. Listeners can opt for a one-time only
 * notification or to be notified repeatedly.
 */
public interface BufferListener {

	/**
	 * Notification callback if a buffer is recycled and becomes available in buffer pool.
	 *
	 * <p><strong>BEWARE:</strong> This happens under synchronization in {@link LocalBufferPool}.
	 * Therefore, if the implementation requires another lock (for whatever reason), make sure that
	 * no other use of this lock recycles buffers (to the same buffer pool) or you may deadlock
	 * because of the locks being acquired in different order (for an example, see FLINK-9676).
	 *
	 * @param buffer buffer that becomes available in buffer pool.
	 * @return true if the listener wants to be notified next time.
	 */
	boolean notifyBufferAvailable(Buffer buffer);

	/**
	 * Notification callback if the buffer provider is destroyed.
	 */
	void notifyBufferDestroyed();
}
