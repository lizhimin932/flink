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

package org.apache.flink.runtime.io.network.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.NetUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NettyClientServerSslTest {

	private final Logger LOG = LoggerFactory.getLogger(NettyClientServerSslTest.class);

	/**
	 * Verify valid ssl configuration and connection
	 *
	 */
	@Test
	public void testValidSslConnection() throws Exception {
		NettyProtocol protocol = new NettyProtocol() {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new ChannelHandler[0];
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() { return new ChannelHandler[0]; }
		};

		NettyConfig nettyConfig = new NettyConfig(
			InetAddress.getLoopbackAddress(),
			NetUtils.getAvailablePort(),
			NettyTestUtil.DEFAULT_SEGMENT_SIZE,
			1,
			createSslConfig(true));

		NettyTestUtil.NettyServerAndClient serverAndClient = NettyTestUtil.initServerAndClient(protocol, nettyConfig);

		Channel ch = NettyTestUtil.connect(serverAndClient);

		// should be able to send text data
		ch.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());
		assertTrue(ch.writeAndFlush("test-1").await().isSuccess());

		ch.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());
		assertTrue(ch.writeAndFlush("test-2").await().isSuccess());

		//Create another client and connect to server. It should get authenticated first
		NettyBufferPool bufferPool = new NettyBufferPool(1);
		final NettyClient client = NettyTestUtil.initClient(nettyConfig, protocol, bufferPool);
		Channel channel = NettyTestUtil.connect(client, serverAndClient.server());

		// should be able to send text data
		channel.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());
		assertTrue(channel.writeAndFlush("bar-1").await().isSuccess());

		channel.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());
		assertTrue(channel.writeAndFlush("bar-2").await().isSuccess());

		client.shutdown();

		NettyTestUtil.shutdown(serverAndClient);
	}

	/**
	 * Verify failure on invalid ssl configuration
	 *
	 */
	@Test
	public void testInvalidSslConfiguration() throws Exception {
		NettyProtocol protocol = new NettyProtocol() {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new ChannelHandler[0];
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() { return new ChannelHandler[0]; }
		};

		Configuration config = createSslConfig(true);
		// Modify the keystore password to an incorrect one
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "invalidpassword");

		NettyConfig nettyConfig = new NettyConfig(
			InetAddress.getLoopbackAddress(),
			NetUtils.getAvailablePort(),
			NettyTestUtil.DEFAULT_SEGMENT_SIZE,
			1,
			config);

		NettyTestUtil.NettyServerAndClient serverAndClient = null;
		try {
			serverAndClient = NettyTestUtil.initServerAndClient(protocol, nettyConfig);
			Assert.fail("Created server and client from invalid configuration");
		} catch (Exception e) {
			// Exception should be thrown as expected
		}

		NettyTestUtil.shutdown(serverAndClient);
	}

	/**
	 * Verify SSL handshake error when untrusted server certificate is used
	 *
	 */
	@Test
	public void testSslHandshakeError() throws Exception {
		NettyProtocol protocol = new NettyProtocol() {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new ChannelHandler[0];
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() { return new ChannelHandler[0]; }
		};

		Configuration config = createSslConfig(true);

		// Use a server certificate which is not present in the truststore
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/untrusted.keystore");

		NettyConfig nettyConfig = new NettyConfig(
			InetAddress.getLoopbackAddress(),
			NetUtils.getAvailablePort(),
			NettyTestUtil.DEFAULT_SEGMENT_SIZE,
			1,
			config);

		NettyTestUtil.NettyServerAndClient serverAndClient = NettyTestUtil.initServerAndClient(protocol, nettyConfig);

		Channel ch = NettyTestUtil.connect(serverAndClient);
		ch.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());

		// Attempting to write data over ssl should fail
		assertFalse(ch.writeAndFlush("test").await().isSuccess());

		NettyTestUtil.shutdown(serverAndClient);
	}

	/**
	 * Verify client gets disconnected when valid secure cookie is not provided
	 *
	 */
	@Test
	public void testValidSslAndInvalidCookie() throws Exception {

		NettyProtocol protocol = new NettyProtocol() {
			@Override
			public ChannelHandler[] getServerChannelHandlers() {
				return new ChannelHandler[0];
			}

			@Override
			public ChannelHandler[] getClientChannelHandlers() {
				return new ChannelHandler[0];
			}
		};

		NettyBufferPool bufferPool = new NettyBufferPool(1);

		NettyConfig nettyConfig = new NettyConfig(
				InetAddress.getLoopbackAddress(),
				NetUtils.getAvailablePort(),
				NettyTestUtil.DEFAULT_SEGMENT_SIZE,
				1,
				createSslConfig(true));

		final NettyServer server = NettyTestUtil.initServer(nettyConfig, protocol, bufferPool);

		//Create client with invalid secure cookie and it should fail
		NettyConfig nettyConfigWithInvalidCookie = new NettyConfig(
				InetAddress.getLoopbackAddress(),
				NetUtils.getAvailablePort(),
				NettyTestUtil.DEFAULT_SEGMENT_SIZE,
				1,
				createSslConfig(false));

		final NettyClient client = NettyTestUtil.initClient(nettyConfigWithInvalidCookie, protocol, bufferPool);
		Channel channel = NettyTestUtil.connect(client, server);
		channel.pipeline().addLast(new StringDecoder()).addLast(new StringEncoder());
		ChannelFuture future = channel.writeAndFlush("blah").await();
		Thread.sleep(500);
		assertFalse(future.channel().isActive());

		client.shutdown();
		server.shutdown();
	}

	private Configuration createSslConfig(boolean enableCookie) throws Exception {

		Configuration flinkConfig = new Configuration();
		flinkConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password");
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password");
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		flinkConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");

		if(enableCookie) {
			flinkConfig.setBoolean(ConfigConstants.SECURITY_ENABLED, true);
			flinkConfig.setString(ConfigConstants.SECURITY_COOKIE, "foo");
		}

		return flinkConfig;
	}
}
