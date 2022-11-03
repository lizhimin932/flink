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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Builder for the {@link TestingResourceAllocator}. */
public class TestingResourceAllocatorBuilder {
    private BiConsumer<InstanceID, Exception> releaseResourceConsumer = (ignoredA, ignoredB) -> {};
    private Consumer<WorkerResourceSpec> allocateResourceConsumer = (ignored) -> {};

    public TestingResourceAllocatorBuilder setReleaseResourceConsumer(
            BiConsumer<InstanceID, Exception> releaseResourceConsumer) {
        this.releaseResourceConsumer = releaseResourceConsumer;
        return this;
    }

    public TestingResourceAllocatorBuilder setAllocateResourceConsumer(
            Consumer<WorkerResourceSpec> allocateResourceConsumer) {
        this.allocateResourceConsumer = allocateResourceConsumer;
        return this;
    }

    public TestingResourceAllocator build() {
        return new TestingResourceAllocator(releaseResourceConsumer, allocateResourceConsumer);
    }
}
