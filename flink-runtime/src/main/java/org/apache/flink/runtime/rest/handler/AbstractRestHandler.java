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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Super class for netty-based handlers that work with {@link RequestBody}s and {@link ResponseBody}s.
 *
 * <p>Subclasses must be thread-safe.
 *
 * @param <R> type of incoming requests
 * @param <P> type of outgoing responses
 */
@ChannelHandler.Sharable
public abstract class AbstractRestHandler<R extends RequestBody, P extends ResponseBody> extends SimpleChannelInboundHandler<Routed> {
	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final ObjectMapper mapper = RestMapperUtils.getStrictObjectMapper();

	private final MessageHeaders<R, P> messageHeaders;

	protected AbstractRestHandler(MessageHeaders<R, P> messageHeaders) {
		this.messageHeaders = messageHeaders;
	}

	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, Routed routed) throws Exception {
		log.debug("Received request.");
		final HttpRequest httpRequest = routed.request();

		try {
			if (!(httpRequest instanceof FullHttpRequest)) {
				log.error("Implementation error: Received a request that wasn't a FullHttpResponse.");
				sendErrorResponse(new ErrorResponseBody("Bad request received."), HttpResponseStatus.BAD_REQUEST, ctx, httpRequest);
				return;
			}

			ByteBuf msgContent = ((FullHttpRequest) httpRequest).content();

			R request;
			try {
				if (msgContent.capacity() == 0 || messageHeaders.getHttpMethod() == HttpMethodWrapper.GET) {
					request = mapper.readValue("{}", messageHeaders.getRequestClass());
				} else {
					ByteBufInputStream in = new ByteBufInputStream(msgContent);
					request = mapper.readValue(in, messageHeaders.getRequestClass());
				}
			} catch (JsonParseException | JsonMappingException je) {
				log.error("Failed to read request.", je);
				sendErrorResponse(new ErrorResponseBody(String.format("Request did not match expected format %s.", messageHeaders.getRequestClass().getSimpleName())), HttpResponseStatus.BAD_REQUEST, ctx, httpRequest);
				return;
			}

			CompletableFuture<HandlerResponse<P>> response;
			try {
				HandlerRequest<R> handlerRequest = new HandlerRequest<>(request, routed.pathParams(), routed.queryParams());
				response = handleRequest(handlerRequest);
			} catch (Exception e) {
				response = FutureUtils.completedExceptionally(e);
			}

			response.whenComplete((HandlerResponse<P> resp, Throwable error) -> {
				try {
					if (error != null) {
						log.error("Implementation error: Unhandled exception.", error);
						sendErrorResponse(new ErrorResponseBody("Internal server error."), HttpResponseStatus.INTERNAL_SERVER_ERROR, ctx, httpRequest);
					} else {
						if (resp.wasSuccessful()) {
							sendResponse(messageHeaders, resp.getResponse(), ctx, httpRequest);
						} else {
							sendErrorResponse(new ErrorResponseBody(resp.getErrorMessage()), resp.getErrorCode(), ctx, httpRequest);
						}
					}
				} catch (Exception e) {
					log.error("Critical error while sending a response.", e);
				}
			});
		} catch (Exception e) {
			log.error("Request processing failed.", e);
			try {
				sendErrorResponse(new ErrorResponseBody("Internal server error."), HttpResponseStatus.INTERNAL_SERVER_ERROR, ctx, httpRequest);
			} catch (IOException e1) {
				log.error("Critical error while sending a response.", e1);
			}
		}
	}

	/**
	 * This method is called for every incoming request and returns a {@link HandlerResponse} that either contains a
	 * {@link ResponseBody} of type {@code P} if the request was handled successfully or an error otherwise.
	 *
	 * @param request request that should be handled
	 * @return future containing a handler response
	 */
	protected abstract CompletableFuture<HandlerResponse<P>> handleRequest(@Nonnull HandlerRequest<R> request);

	private static <R extends RequestBody, P extends ResponseBody> void sendResponse(MessageHeaders<R, P> messageHeaders, P response, ChannelHandlerContext ctx, HttpRequest httpRequest) throws IOException {
		StringWriter sw = new StringWriter();
		mapper.writeValue(sw, response);
		sendResponse(ctx, httpRequest, messageHeaders.getResponseStatusCode(), sw.toString());
	}

	private static void sendErrorResponse(ErrorResponseBody error, HttpResponseStatus statusCode, ChannelHandlerContext ctx, HttpRequest httpRequest) throws IOException {
		StringWriter sw = new StringWriter();
		mapper.writeValue(sw, error);
		sendResponse(ctx, httpRequest, statusCode, sw.toString());
	}

	private static void sendResponse(@Nonnull ChannelHandlerContext ctx, @Nonnull HttpRequest httpRequest, @Nonnull HttpResponseStatus statusCode, @Nonnull String message) {
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, statusCode);

		response.headers().set(CONTENT_TYPE, "application/json");

		if (HttpHeaders.isKeepAlive(httpRequest)) {
			response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
		}

		byte[] buf = message.getBytes(ConfigConstants.DEFAULT_CHARSET);
		ByteBuf b = Unpooled.copiedBuffer(buf);
		HttpHeaders.setContentLength(response, buf.length);

		// write the initial line and the header.
		ctx.write(response);

		ctx.write(b);

		ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

		// close the connection, if no keep-alive is needed
		if (!HttpHeaders.isKeepAlive(httpRequest)) {
			lastContentFuture.addListener(ChannelFutureListener.CLOSE);
		}
	}
}
