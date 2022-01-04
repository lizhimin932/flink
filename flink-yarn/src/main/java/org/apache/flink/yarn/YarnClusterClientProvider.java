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

package org.apache.flink.yarn;

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.yarn.YarnClusterDescriptor.setClusterEntrypointInfoToConfig;

/** The implementation of ClusterClientProvider. */
@Internal
public class YarnClusterClientProvider implements ClusterClientProvider {
    private static final Logger LOG = LoggerFactory.getLogger(YarnClusterClientProvider.class);

    private final Object lock = new Object();

    private volatile ApplicationId applicationId;
    private ApplicationReportProvider appReportProvider;
    private Configuration flinkConf;

    private YarnClusterClientProvider(
            ApplicationReportProvider applicationReportProvider, Configuration flinkConfiguration) {
        this.appReportProvider = applicationReportProvider;
        this.flinkConf = flinkConfiguration;
    }

    @Override
    public ClusterClient getClusterClient() {
        try {
            if (applicationId != null) {
                synchronized (lock) {
                    if (applicationId != null) {
                        ApplicationReport report = appReportProvider.waitTillSubmissionFinish();
                        this.applicationId = report.getApplicationId();
                        setClusterEntrypointInfoToConfig(flinkConf, report);
                    }
                }
            }
            return new RestClusterClient(flinkConf, applicationId);
        } catch (Exception e) {
            throw new RuntimeException("Errors on getting Yarn cluster client.", e);
        }
    }

    static YarnClusterClientProvider of(
            ApplicationReportProvider applicationReportProvider,
            final Configuration flinkConfiguration) {
        return new YarnClusterClientProvider(applicationReportProvider, flinkConfiguration);
    }
}
