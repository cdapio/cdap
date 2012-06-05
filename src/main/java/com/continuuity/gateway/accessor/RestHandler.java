package com.continuuity.gateway.accessor;

import com.continuuity.data.operation.Read;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the http request handler for the rest accessor. At this time it only accepts
 * GET requests to retrieve a value for a key from a named table.
 */
public class RestHandler extends SimpleChannelUpstreamHandler {

	private static final Logger LOG = LoggerFactory
			.getLogger(RestHandler.class);

	/**
	 * Will help validate URL paths, and also has the name of the connector and
	 * the data fabric executor.
	 */
	private RestAccessor accessor;

	/**
	 * All the paths have to be of the form http://host:port&lt;pathPrefix>&lt;table>/&lt;key>
	 * For instance, if config(prefix="/v0.1/" path="table/"), then pathPrefix will be
	 * "/v0.1/table/", and a valid request is GET http://host:port/v0.1/table/mytable/12345678
	 */
	private String pathPrefix;

	/** Disallow default constructor */
	private RestHandler() { }

	/**
	 * Constructor requires the accessor that created this
	 * @param accessor  the accessor that created this
	 */
	RestHandler(RestAccessor accessor) {
		this.accessor = accessor;
		this.pathPrefix = accessor.getHttpConfig().getPrefix() + accessor.getHttpConfig().getPath();
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

		// we only support get requests for now
		HttpMethod method = request.getMethod();
		if (method != HttpMethod.GET) {
			LOG.info("Received a " + method + " request, which is not supported");
			respondError(message.getChannel(), HttpResponseStatus.METHOD_NOT_ALLOWED);
			// @todo according to HTTP 1.1 spec we must return an ALLOW header
			return;
		}

		// we do not support a query or parameters in the URL
		QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
		if (decoder.getParameters() != null && !decoder.getParameters().isEmpty()) {
			LOG.info("Received a request with query parameters, which is not supported");
			respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
			return;
		}

		// we only support requests of the form POST http://host:port/prefix/path/<tablename>/<key>
		String destination = null, key = null;
		String path = decoder.getPath();
		if (path.startsWith(this.pathPrefix)) {
			String remainder = path.substring(this.pathPrefix.length());
			int pos = remainder.indexOf("/");
			if (pos > 0) {
				destination = remainder.substring(0, pos);
				if ("default".equals(destination)) {
					key = remainder.substring(pos + 1);
				}
			}
		}
		if (key == null) {
			LOG.info("Received a request with invalid path " + path);
			respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
			return;
		}

		// ignore the content of the request - this is a GET
		// get the value from the data fabric
		byte[] value = null;
		try {
			Read read = new Read(key.getBytes());
			value = this.accessor.getExecutor().execute(read);
		} catch (Exception e) {
			LOG.error("Error reading value for key '" + key + "': " + e.getMessage() + ".", e);
			respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
			return;
		}
		if (value == null) {
			respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
		} else {
			respondSuccess(message.getChannel(), request, value);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		LOG.error("Exception caught for connector '" + this.accessor.getName() + "'. ", e.getCause());
		e.getChannel().close();
	}
}
