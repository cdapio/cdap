package com.continuuity.gateway.collector;

import com.continuuity.flow.flowlet.api.Event;
import com.continuuity.flow.flowlet.impl.EventBuilder;
import com.continuuity.gateway.Constants;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

// @todo write javadoc
public class RestHandler extends SimpleChannelUpstreamHandler {

	private static final Logger LOG = LoggerFactory
			.getLogger(RestHandler.class);

	private RestCollector collector;
	private String pathPrefix;

	private RestHandler() { };
	RestHandler(RestCollector collector) {
		this.collector = collector;
		this.pathPrefix = collector.getHttpConfig().getPrefix()
				+ collector.getHttpConfig().getPath();
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
	 */
	private void respondSuccess(Channel channel, HttpRequest request) {
		HttpResponse response = new DefaultHttpResponse(
				HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		boolean keepAlive = HttpHeaders.isKeepAlive(request);
		response.addHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
		ChannelFuture future = channel.write(response);
		if (!keepAlive) {
			future.addListener(ChannelFutureListener.CLOSE);
		}
	}

	private String isPreservedHeader(String destinationPrefix, String name) {
		if (Constants.HEADER_CLIENT_TOKEN.equals(name)) return name;
		if (name.startsWith(destinationPrefix)) return name.substring(destinationPrefix.length());
		return null;
	}

	@Override
	public void messageReceived(ChannelHandlerContext context, MessageEvent message) throws Exception {
		HttpRequest request = (HttpRequest) message.getMessage();

		LOG.info("Request received");

		HttpMethod method = request.getMethod();
		if (method != HttpMethod.POST) {
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

		EventBuilder builder = new EventBuilder();
		builder.setHeader(Constants.HEADER_FROM_COLLECTOR, this.collector.getName());
		builder.setHeader(Constants.HEADER_DESTINATION_ENDPOINT, destination);

		String prefix = destination + ".";
		Set<String> headers = request.getHeaderNames();
		for (String header : headers) {
			String preservedHeader = isPreservedHeader(prefix, header);
			if (preservedHeader != null) {
				builder.setHeader(preservedHeader, request.getHeader(header));
			}
		}

		ChannelBuffer content = request.getContent();
		int length = content.readableBytes();
		if (length > 0) {
			byte[] bytes = new byte[length];
			content.readBytes(bytes);
			builder.setBody(bytes);
		}
		Event event = builder.create();

		try {
			this.collector.getConsumer().consumeEvent(event);
		} catch (Exception e) {
			LOG.warn("Error consuming single event: " + e.getMessage());
			respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
			return;
		}

		respondSuccess(message.getChannel(), request);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		LOG.error("Exception caught for collector '" + this.collector.getName() + "'. ", e.getCause());
		e.getChannel().close();
	}
}
