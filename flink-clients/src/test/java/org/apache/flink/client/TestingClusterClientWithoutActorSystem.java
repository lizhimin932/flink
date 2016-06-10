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
package org.apache.flink.client;

import akka.actor.ActorSystem;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * A client to use in tests which does not instantiate an ActorSystem.
 */
public class TestingClusterClientWithoutActorSystem extends StandaloneClusterClient {

	private TestingClusterClientWithoutActorSystem() throws IOException {
		super(new Configuration());
	}

	/**
	 * Do not instantiate the Actor System to save resources.
	 * @return Mocked ActorSystem
	 * @throws IOException
	 */
	@Override
	protected ActorSystem createActorSystem() throws IOException {
		return Mockito.mock(ActorSystem.class);
	}

	public static ClusterClient create() {
		try {
			return new TestingClusterClientWithoutActorSystem();
		} catch (IOException e) {
			throw new RuntimeException("Could not create TestingClientWithoutActorSystem.", e);
		}
	}

}
