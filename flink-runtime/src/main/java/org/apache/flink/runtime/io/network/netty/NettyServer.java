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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Low-level Netty server.
 */
public class NettyServer {

	private static final ThreadFactoryBuilder THREAD_FACTORY_BUILDER = new ThreadFactoryBuilder().setDaemon(true);

	private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

	private final NettyConfig config;

	private ServerBootstrap bootstrap;

	private ChannelFuture bindFuture;

	private InetSocketAddress localAddress;

	public NettyServer(NettyConfig config) {
		this.config = checkNotNull(config);
		localAddress = null;
	}

	public void init(final NettyProtocol protocol, ByteBufAllocator nettyBufferPool) throws IOException {
		checkState(bootstrap == null, "Netty server has already been initialized.");
		checkNotNull(protocol, "protocol");

		long start = System.currentTimeMillis();

		bootstrap = new ServerBootstrap();

		// --------------------------------------------------------------------
		// Transport-specific configuration
		// --------------------------------------------------------------------

		switch (config.getTransportType()) {
			case NIO:
				initNioBootstrap();
				break;

			case EPOLL:
				initEpollBootstrap();
				break;

			case AUTO:
				if (Epoll.isAvailable()) {
					initEpollBootstrap();
					LOG.info("Transport type 'auto': using EPOLL.");
				}
				else {
					initNioBootstrap();
					LOG.info("Transport type 'auto': using NIO.");
				}
		}

		// --------------------------------------------------------------------
		// Configuration
		// --------------------------------------------------------------------

		// Server bind address
		bootstrap.localAddress(config.getServerAddress(), config.getServerPort());

		// Pooled allocators for Netty's ByteBuf instances
		bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);
		bootstrap.childOption(ChannelOption.ALLOCATOR, nettyBufferPool);

		if (config.getServerConnectBacklog() > 0) {
			bootstrap.option(ChannelOption.SO_BACKLOG, config.getServerConnectBacklog());
		}

		// Receive and send buffer size
		int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
		if (receiveAndSendBufferSize > 0) {
			bootstrap.childOption(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
			bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
		}

		// Low and high water marks for flow control
		Tuple2<Integer, Integer> watermark = config.getServerWriteBufferWatermark();
		if(watermark != null) {
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, watermark.f0);
			bootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, watermark.f1);
		}

		// --------------------------------------------------------------------
		// Child channel pipeline for accepted connections
		// --------------------------------------------------------------------
		bootstrap.childHandler(protocol.getServerChannelHandler());

		// --------------------------------------------------------------------
		// Start Server
		// --------------------------------------------------------------------

		bindFuture = bootstrap.bind().syncUninterruptibly();

		localAddress = (InetSocketAddress) bindFuture.channel().localAddress();

		long end = System.currentTimeMillis();
		LOG.info("Successful initialization (took {} ms). Listening on SocketAddress {}.", (end - start), bindFuture.channel().localAddress().toString());
	}

	NettyConfig getConfig() {
		return config;
	}

	ServerBootstrap getBootstrap() {
		return bootstrap;
	}

	public InetSocketAddress getLocalAddress() {
		return localAddress;
	}

	public void shutdown() {
		long start = System.currentTimeMillis();
		if (bindFuture != null) {
			bindFuture.channel().close().awaitUninterruptibly();
			bindFuture = null;
		}

		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
			bootstrap = null;
		}
		long end = System.currentTimeMillis();
		LOG.info("Successful shutdown (took {} ms).", (end - start));
	}

	private void initNioBootstrap() {
		String name = config.getServerThreadGroupName();
		NioEventLoopGroup nioGroup = new NioEventLoopGroup(config.getServerNumThreads(), getNamedThreadFactory(name));
		bootstrap.group(nioGroup).channel(NioServerSocketChannel.class);
	}

	private void initEpollBootstrap() {
		String name = config.getServerThreadGroupName();
		EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(config.getServerNumThreads(), getNamedThreadFactory(name));
		bootstrap.group(epollGroup).channel(EpollServerSocketChannel.class);
	}

	public static ThreadFactory getNamedThreadFactory(String name) {
		return THREAD_FACTORY_BUILDER.setNameFormat(name + " Thread %d").build();
	}
}
