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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.rpc.FatalErrorHandler;

/**
 * Testing implementation of {@link MultipleComponentLeaderElectionDriverFactory} that returns a
 * given {@link MultipleComponentLeaderElectionDriver}.
 */
public class TestingMultipleComponentLeaderElectionDriverFactory
        implements MultipleComponentLeaderElectionDriverFactory {

    final TestingMultipleComponentLeaderElectionDriver testingMultipleComponentLeaderElectionDriver;

    public TestingMultipleComponentLeaderElectionDriverFactory(
            TestingMultipleComponentLeaderElectionDriver
                    testingMultipleComponentLeaderElectionDriver) {
        this.testingMultipleComponentLeaderElectionDriver =
                testingMultipleComponentLeaderElectionDriver;
    }

    @Override
    public MultipleComponentLeaderElectionDriver create(
            MultipleComponentLeaderElectionDriver.Listener leaderElectionListener,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {
        testingMultipleComponentLeaderElectionDriver.setListener(leaderElectionListener);
        testingMultipleComponentLeaderElectionDriver.setFatalErrorHandler(fatalErrorHandler);

        return testingMultipleComponentLeaderElectionDriver;
    }
}
