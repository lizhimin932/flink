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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.yarn.YarnConfigKeys.ENV_APP_ID;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_HOME_DIR;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_SHIP_FILES;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_HADOOP_USER_NAME;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_JAR_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link YarnApplicationMasterRunner}.
 */
@PrepareForTest(FileSystem.class)
@RunWith(PowerMockRunner.class)
public class YarnApplicationMasterRunnerTest {
	private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMasterRunnerTest.class);

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@BeforeClass
	public static void checkOS() {
		Assume.assumeTrue(!OperatingSystem.isWindows());
	}

	@Test
	public void testCreateTaskExecutorContext() throws Exception {
		File root = folder.getRoot();
		File home = new File(root, "home");
		boolean created = home.mkdir();
		assertTrue(created);

		Answer<?> getDefault = new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				return invocationOnMock.getArguments()[1];
			}
		};
		Configuration flinkConf = new Configuration();
		YarnConfiguration yarnConf = mock(YarnConfiguration.class);
		doAnswer(getDefault).when(yarnConf).get(anyString(), anyString());
		doAnswer(getDefault).when(yarnConf).getInt(anyString(), anyInt());
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				return new String[] {(String) invocationOnMock.getArguments()[1]};
			}
		}).when(yarnConf).getStrings(anyString(), Mockito.<String> anyVararg());

		Map<String, String> env = new HashMap<>();
		env.put(ENV_APP_ID, "foo");
		env.put(ENV_CLIENT_HOME_DIR, home.getAbsolutePath());
		env.put(ENV_CLIENT_SHIP_FILES, "");
		env.put(ENV_FLINK_CLASSPATH, "");
		env.put(ENV_HADOOP_USER_NAME, "foo");
		env.put(FLINK_JAR_PATH, root.toURI().toString());
		env = Collections.unmodifiableMap(env);

		ContaineredTaskManagerParameters tmParams = mock(ContaineredTaskManagerParameters.class);
		Configuration taskManagerConf = new Configuration();

		String workingDirectory = root.getAbsolutePath();
		Class<?> taskManagerMainClass = YarnApplicationMasterRunnerTest.class;
		ContainerLaunchContext ctx = Utils.createTaskExecutorContext(flinkConf, yarnConf, env, tmParams,
			taskManagerConf, workingDirectory, taskManagerMainClass, LOG);
		assertEquals("file", ctx.getLocalResources().get("flink.jar").getResource().getScheme());
	}

	@Test
	public void testRunAndInitializeFileSystem() throws Exception {
		// Mock necessary system variables
		Map<String, String> map = new HashMap<String, String>(System.getenv());
		map.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, "foo");
		// Create dynamic properties to be used in the Flink configuration
		map.put(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES, "myKey=myValue");
		CommonTestUtils.setEnv(map);

		// Create a temporary flink-conf.yaml and to be deleted on JVM exits
		File currDir = new File(System.getenv().get(ApplicationConstants.Environment.PWD.key()));
		String path = String.format("%s/%s.%s", currDir, "flink-conf", "yaml");
		File f = new File(path);
		f.createNewFile();
		f.deleteOnExit();

		// Mock FileSystem.initialize()
		PowerMockito.mockStatic(FileSystem.class);
		PowerMockito.doNothing().when(FileSystem.class);
		FileSystem.initialize(any(Configuration.class));

		String[] args = new String[5];
		YarnApplicationMasterRunner yarnApplicationMasterRunner = new YarnApplicationMasterRunner();
		yarnApplicationMasterRunner.run(args);

		// Verify FileSystem.initialize() is invoked with the correct Flink config
		ArgumentCaptor<Configuration> propertiesCaptor =
			ArgumentCaptor.forClass(Configuration.class);
		PowerMockito.verifyStatic();
		FileSystem.initialize(propertiesCaptor.capture());
		assertEquals("myValue", propertiesCaptor.getValue().getString("myKey", ""));
	}
}
