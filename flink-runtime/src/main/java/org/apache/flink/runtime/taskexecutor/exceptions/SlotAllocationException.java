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

package org.apache.flink.runtime.taskexecutor.exceptions;

/** Exception indicating that the slot allocation on the task manager failed. */
public class SlotAllocationException extends TaskManagerException {

    private static final long serialVersionUID = -4764932098204266773L;

    public SlotAllocationException(String message) {
        super(message);
    }

    public SlotAllocationException(String message, Throwable cause) {
        super(message, cause);
    }

    public SlotAllocationException(Throwable cause) {
        super(cause);
    }
}
