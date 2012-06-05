package com.continuuity.gateway.accessor;

import com.continuuity.gateway.util.HttpConfig;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// @todo write javadoc
public class RestHandler extends SimpleChannelUpstreamHandler {
	private static final Logger LOG = LoggerFactory
			.getLogger(RestHandler.class);

	private HttpConfig config;
	private String pathPrefix;

	private RestHandler() { };
	RestHandler(HttpConfig config) {
		this.config = config;
		this.pathPrefix = this.config.getPrefix()	+ this.config.getPath();
	}

	/**
	 * Respond to the client with an error. That closes the connection.
	 * @param channel the channel on which the request came
	 * @param status the HTTP status to return
	 */
	private void respondError(Channel channel, HttpResponseStatus status) {
		HttpResponse response = new DefaultHttpResponse(
				HttpVersion.HTTP_1_1, status);
		ChannelFuture future = channel.write(response);
		future.addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * Respond to the client with success. This keeps the connection alive
	 * unless specified otherwise in the original request.
	 * @param channel the channel on which the request came
	 * @param request the original request (to determine whether to keep the connection alive)
	 * @param content the content of the response to send
	 */
	private void respondSuccess(Channel channel, HttpRequest request, byte[] content) {
		HttpResponse response = new DefaultHttpResponse(
				HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		boolean keepAlive = HttpHeaders.isKeepAlive(request);
		response.addHeader(HttpHeaders.Names.CONTENT_LENGTH, content.length);
		response.setContent(ChannelBuffers.wrappedBuffer(content));
		ChannelFuture future = channel.write(response);
		if (!keepAlive) {
			future.addListener(ChannelFutureListener.CLOSE);
		}
	}

	@Override
	public void messageReceived(ChannelHandlerContext context, MessageEvent message) throws Exception {
		HttpRequest request = (HttpRequest) message.getMessage();

		LOG.info("Request received");

		HttpMethod method = request.getMethod();
		if (method != HttpMethod.GET) {
			LOG.info("Received a " + method + " request, which is not supported");
			respondError(message.getChannel(), HttpResponseStatus.METHOD_NOT_ALLOWED);
			// @todo according to HTTP 1.1 spec we must return an ALLOW header
			return;
		}

		QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
		if (decoder.getParameters() != null && !decoder.getParameters().isEmpty()) {
			LOG.info("Received a request with query parameters, which is not supported");
			respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
			return;
		}

		String destination = null;
		String path = decoder.getPath();

		if (path.startsWith(this.pathPrefix)) {
			String resourceName = path.substring(this.pathPrefix.length());
			if (!resourceName.contains("/")) {
				destination = resourceName;
			}
		}
		if (destination == null) {
			LOG.info("Received a request with invalid path " + path);
			respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
			return;
		}

		// ignore the content of the request - this is a GET
		// get the value from the data fabric
		byte[] value = null;
		try {
			// @todo perform get from data fab fabric
		} catch (Exception e) {
			LOG.warn("Error consuming single event: " + e.getMessage());
			respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
			return;
		}
		respondSuccess(message.getChannel(), request, value);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		LOG.error("Exception caught for connector '" + this.config.getName() + "'. ", e.getCause());
		e.getChannel().close();
	}
}
