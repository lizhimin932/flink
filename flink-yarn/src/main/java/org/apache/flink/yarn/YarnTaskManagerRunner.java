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

package org.apache.flink.yarn;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.modules.HadoopModule;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * The entry point for running a TaskManager in a YARN container.
 */
public class YarnTaskManagerRunner {

	private static final Logger LOG = LoggerFactory.getLogger(YarnTaskManagerRunner.class);

	private static Callable<Object> createMainRunner(
			Configuration configuration,
			ResourceID resourceId,
			final Class<? extends YarnTaskManager> taskManager) {
		return new Callable<Object>() {
			@Override
			public Integer call() {
				try {
					TaskManager.selectNetworkInterfaceAndRunTaskManager(
							configuration, resourceId, taskManager);
				} catch (Throwable t) {
					LOG.error("Error while starting the TaskManager", t);
					System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
				}
				return null;
			}
		};
	}

	public static void runYarnTaskManager(String[] args,
																				final Class<? extends YarnTaskManager> taskManager,
																				Map<String, String> envs,
																				Callable<Object> mainRunner) throws IOException {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN TaskManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// try to parse the command line arguments
		final Configuration configuration;
		try {
			configuration = TaskManager.parseArgsAndLoadConfig(args);
		}
		catch (Throwable t) {
			LOG.error(t.getMessage(), t);
			System.exit(TaskManager.STARTUP_FAILURE_RETURN_CODE());
			return;
		}

		// read the environment variables for YARN
		final String yarnClientUsername = envs.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
		final String localDirs = envs.get(Environment.LOCAL_DIRS.key());
		LOG.info("Current working/local Directory: {}", localDirs);

		final String currDir = envs.get(Environment.PWD.key());
		LOG.info("Current working Directory: {}", currDir);

		final String remoteKeytabPrincipal = envs.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		LOG.info("TM: remoteKeytabPrincipal obtained {}", remoteKeytabPrincipal);

		// configure local directory
		if (configuration.contains(CoreOptions.TMP_DIRS)) {
			LOG.info("Overriding YARN's temporary file directories with those " +
				"specified in the Flink config: " + configuration.getValue(CoreOptions.TMP_DIRS));
		}
		else {
			LOG.info("Setting directories for temporary files to: {}", localDirs);
			configuration.setString(CoreOptions.TMP_DIRS, localDirs);
		}

		// tell akka to die in case of an error
		configuration.setBoolean(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR, true);

		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

		LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
				currentUser.getShortUserName(), yarnClientUsername);

		// Infer the resource identifier from the environment variable
		String containerID = Preconditions.checkNotNull(envs.get(YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID));
		final ResourceID resourceId = new ResourceID(containerID);
		LOG.info("ResourceID assigned for this container: {}", resourceId);

		File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
		if (remoteKeytabPrincipal != null && f.exists()) {
			// set keytab principal and replace path with the local path of the shipped keytab file in NodeManager
			configuration.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, f.getAbsolutePath());
			configuration.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
		}
		try {

			SecurityConfiguration sc;

			//To support Yarn Secure Integration Test Scenario
			File krb5Conf = new File(currDir, Utils.KRB5_FILE_NAME);
			if (krb5Conf.exists() && krb5Conf.canRead()) {
				String krb5Path = krb5Conf.getAbsolutePath();
				LOG.info("KRB5 Conf: {}", krb5Path);
				org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();
				hadoopConfiguration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
				hadoopConfiguration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");

				sc = new SecurityConfiguration(configuration,
					Collections.singletonList(securityConfig -> new HadoopModule(securityConfig, hadoopConfiguration)));

			} else {
				sc = new SecurityConfiguration(configuration);

			}

			SecurityUtils.install(sc);

			if (mainRunner == null) {
				mainRunner = createMainRunner(configuration, resourceId, taskManager);
			}
			SecurityUtils.getInstalledContext().runSecured(mainRunner);
		} catch (Exception e) {
			LOG.error("Exception occurred while launching Task Manager", e);
			throw new RuntimeException(e);
		}

	}
}
