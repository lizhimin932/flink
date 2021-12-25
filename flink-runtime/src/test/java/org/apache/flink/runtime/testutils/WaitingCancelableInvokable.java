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

package org.apache.flink.runtime.testutils;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

/** An {@link AbstractInvokable} which waits until it is canceled. */
public class WaitingCancelableInvokable extends CancelableInvokable {

    private final Object lock = new Object();
    private boolean running = true;

    public WaitingCancelableInvokable(Environment environment) {
        super(environment);
    }

    @Override
    public void doInvoke() throws Exception {
        synchronized (lock) {
            while (running) {
                lock.wait();
            }
        }
    }

    @Override
    public void cancel() {
        synchronized (lock) {
            running = false;
            lock.notifyAll();
        }
    }
}
