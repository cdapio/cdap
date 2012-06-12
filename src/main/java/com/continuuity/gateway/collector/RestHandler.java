package com.continuuity.gateway.collector;

import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.flow.flowlet.impl.EventBuilder;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.util.NettyRestHandler;

/**
 * This is the http request handler for the rest collector. At this time it only accepts
 * POST requests to send an event to a stream.
 */
public class RestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(RestHandler.class);

  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = {HttpMethod.POST};

  /**
   * The collector that created this handler. It has collector name and the consumer
   */
  private RestCollector collector;
  /**
   * All the paths have to be of the form http://host:port&lt;pathPrefix>&lt;stream>
   * For instance, if config(prefix="/v0.1/" path="stream/"), then pathPrefix will be
   * "/v0.1/stream/", and a valid request is POST http://host:port/v0.1/stream/mystream
   */
  private String pathPrefix;

  /**
   * Disallow default constructor
   */
  private RestHandler() {
  }

  /**
   * Constructor requires to pass in the collector that created this handler.
   *
   * @param collector The collector that created this handler
   */
  RestHandler(RestCollector collector) {
    this.collector = collector;
    this.pathPrefix = collector.getHttpConfig().getPathPrefix()
        + collector.getHttpConfig().getPathMiddle();
  }

  /**
   * Determines whether an HTTP header should be preserved in the persisted event,
   * and if so returns the (possibly transformed) header name. We pass through
   * all headers that start with the name of destination stream, but we strip of
   * the stream name.
   *
   * @param destinationPrefix The name of the destination stream with . appended
   * @param name              The nameof the header to check
   * @return the name to use for the header if it is perserved, or null otherwise.
   */
  private String isPreservedHeader(String destinationPrefix, String name) {
    if (Constants.HEADER_CLIENT_TOKEN.equals(name)) return name;
    if (name.startsWith(destinationPrefix)) return name.substring(destinationPrefix.length());
    return null;
  }

  @Override
  public void messageReceived(ChannelHandlerContext context, MessageEvent message) throws Exception {
    HttpRequest request = (HttpRequest) message.getMessage();

    LOG.debug("Request received");

    // we only support POST
    HttpMethod method = request.getMethod();
    if (method != HttpMethod.POST) {
      LOG.debug("Received a " + method + " request, which is not supported");
      respondNotAllowed(message.getChannel(), allowedMethods);
      return;
    }

    // we do not support a query or parameters in the URL
    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    if (decoder.getParameters() != null && !decoder.getParameters().isEmpty()) {
      LOG.debug("Received a request with query parameters, which is not supported");
      respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
      return;
    }

    // does the path of the URL start with the correct prefix, and is it of the form
    // <flowname> or <flowname</<streamname> after that? Otherwise we will not accept this request.
    String destination = null;
    String path = decoder.getPath();
    if (path.startsWith(this.pathPrefix)) {
      String resourceName = path.substring(this.pathPrefix.length());
      if (resourceName.length() > 0) {
        int pos = resourceName.indexOf('/');
        if (pos < 0) { // flowname
          destination = resourceName;
        } else {
          if (pos + 1 == resourceName.length()) {
            destination = resourceName.substring(pos);
          }
          pos = resourceName.indexOf('/', pos + 1);
          if (pos < 0) { // flowname/streamname
            destination = resourceName;
          }
        }
      }
    }
    if (destination == null) {
      LOG.debug("Received a request with invalid path " + path);
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      return;
    }

    // build a new event from the request
    EventBuilder builder = new EventBuilder();
    // set some built-in headers
    builder.setHeader(Constants.HEADER_FROM_COLLECTOR, this.collector.getName());
    builder.setHeader(Constants.HEADER_DESTINATION_STREAM, destination);
    // and transfer all other headers that are to be preserved
    String prefix = destination + ".";
    Set<String> headers = request.getHeaderNames();
    for (String header : headers) {
      String preservedHeader = isPreservedHeader(prefix, header);
      if (preservedHeader != null) {
        builder.setHeader(preservedHeader, request.getHeader(header));
      }
    }

    // read the body of the request and add it to the event
    ChannelBuffer content = request.getContent();
    int length = content.readableBytes();
    if (length > 0) {
      byte[] bytes = new byte[length];
      content.readBytes(bytes);
      builder.setBody(bytes);
    }
    Event event = builder.create();

    // let the consumer process the event.
    // in case of exception, respond with internal error
    try {
      this.collector.getConsumer().consumeEvent(event);
    } catch (Exception e) {
      LOG.error("Error consuming single event: " + e.getMessage());
      respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      return;
    }

    // all good - respond success
    respondSuccess(message.getChannel(), request, null);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
    LOG.error("Exception caught for collector '" + this.collector.getName() + "'. ", e.getCause());
    e.getChannel().close();
  }
}
