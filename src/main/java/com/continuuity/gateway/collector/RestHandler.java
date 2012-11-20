package com.continuuity.gateway.collector;

import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.Event;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.data.operation.ttqueue.*;
import com.continuuity.flow.definition.impl.FlowStream;
import com.continuuity.flow.flowlet.internal.EventBuilder;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.util.MetricsHelper;
import com.continuuity.gateway.util.NettyRestHandler;
import com.google.common.collect.Maps;
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

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.continuuity.gateway.util.MetricsHelper.Status.*;

/**
 * This is the http request handler for the rest collector. This supports
 * POST requests to send an event to a stream, and GET requests to inspect
 * or retrieve events from the stream.
 */
public class RestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(RestHandler.class);

  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = { HttpMethod.POST, HttpMethod.GET };

  /**
   * The collector that created this handler. It has collector name and consumer
   */
  private RestCollector collector;

  /**
   * The metrics object of the rest accessor
   */
  private CMetrics metrics;

  /**
   * All the paths have to be of the form
   * http://host:port&lt;pathPrefix>&lt;stream>
   * For instance, if config(prefix="/v0.1/" path="stream/"),
   * then pathPrefix will be "/v0.1/stream/",
   * and a valid request is POST http://host:port/v0.1/stream/mystream
   */
  private String pathPrefix;

  /**
   * Disallow default constructor
   */
  @SuppressWarnings("unused")
  private RestHandler() {
  }

  /**
   * Constructor requires to pass in the collector that created this handler.
   *
   * @param collector The collector that created this handler
   */
  RestHandler(RestCollector collector) {
    this.collector = collector;
    this.metrics = collector.getMetricsClient();
    this.pathPrefix = collector.getHttpConfig().getPathPrefix()
        + collector.getHttpConfig().getPathMiddle();
  }

  /**
   * Determines whether an HTTP header should be preserved in the persisted
   * event, and if so returns the (possibly transformed) header name. We pass
   * through all headers that start with the name of destination stream, but
   * we strip off the stream name.
   *
   * @param destinationPrefix The name of the destination stream with . appended
   * @param name              The nameof the header to check
   * @return the name to use for the header if it is preserved, null otherwise.
   */
  private String isPreservedHeader(String destinationPrefix, String name) {
    if (Constants.HEADER_CLIENT_TOKEN.equals(name))
      return name;
    if (name.startsWith(destinationPrefix))
      return name.substring(destinationPrefix.length());
    return null;
  }

  private static final int UNKNOWN = 0;
  private static final int ENQUEUE = 1;
  private static final int NEWID = 2;
  private static final int DEQUEUE = 3;
  private static final int META = 4;

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {

    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();

    LOG.trace("Request received: " + method + " " + requestUri);
    MetricsHelper helper = new MetricsHelper(this.getClass(), this.metrics,
        this.collector.getName());

    try {

      // we only support POST and GET
      if (method != HttpMethod.POST && method != HttpMethod.GET ) {
        LOG.trace("Received a " + method + " request, which is not supported");
        helper.finish(BadRequest);
        respondNotAllowed(message.getChannel(), allowedMethods);
        return;
      }

      // we do not support a query or parameters in the URL
      QueryStringDecoder decoder = new QueryStringDecoder(requestUri);
      Map<String, List<String>> parameters = decoder.getParameters();

      int operation = UNKNOWN;
      if (method == HttpMethod.POST) {
        operation = ENQUEUE;
        helper.setMethod("enqueue");
      } else if (method == HttpMethod.GET) {
        if (parameters == null || parameters.size() == 0) {
          operation = META;
          helper.setMethod("getQueueMeta");
        } else {
          List<String> qParams = parameters.get("q");
          if (qParams != null && qParams.size() == 1) {
            if ("newConsumer".equals(qParams.get(0))) {
              operation = NEWID;
              helper.setMethod("getNewId");
            } else if ("dequeue".equals(qParams.get(0))) {
              operation = DEQUEUE;
              helper.setMethod("dequeue");
            }
          }
        }
      }

      // respond with error for unknown requests
      if (operation == UNKNOWN) {
        helper.finish(BadRequest);
        LOG.trace("Received an unsupported " + method +
            " request '" + request.getUri() + "'.");
        respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
        return;
      }

      if ((operation == ENQUEUE || operation == META) &&
          parameters != null && !parameters.isEmpty()) {
        helper.finish(BadRequest);
        LOG.trace(
            "Received a request with query parameters, which is not supported");
        respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
        return;
      }

      // does the path of the URL start with the correct prefix, and is it of
      // the form <flowname> or <flowname</<streamname> after that? Otherwise
      // we will not accept this request.
      String destination = null;
      String path = decoder.getPath();
      if (path.startsWith(this.pathPrefix)) {
        String resourceName = path.substring(this.pathPrefix.length());
        if (resourceName.length() > 0) {
          int pos = resourceName.indexOf('/');
          if (pos < 0) { // streamname
            destination = resourceName;
          } else if (pos + 1 == resourceName.length()) { // streamname/
            destination = resourceName.substring(0, pos);
          }
        }
      }
      if (destination == null) {
        LOG.trace("Received a request with invalid path " + path);
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      } else {
        helper.setScope(destination);
      }

      // validate the existence of the stream
      if (!this.collector.getStreamCache().validateStream(
          OperationContext.DEFAULT_ACCOUNT_ID, destination)) {
        helper.finish(NotFound);
        LOG.trace("Received a request for non-existent stream " + destination);
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }

      switch(operation) {
        case ENQUEUE: {
          // build a new event from the request
          EventBuilder builder = new EventBuilder();
          // set some built-in headers
          builder.setHeader(Constants.HEADER_FROM_COLLECTOR,
              this.collector.getName());
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

          LOG.trace("Sending event to consumer: " + event);
          // let the consumer process the event.
          // in case of exception, respond with internal error
          try {
            this.collector.getConsumer().consumeEvent(event);
          } catch (Exception e) {
            LOG.error("Error consuming single event: " + e.getMessage());
            helper.finish(Error);
            respondError(message.getChannel(),
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
            // refresh cache for this stream - apparently it has disappeared
            this.collector.getStreamCache().refreshStream(
                OperationContext.DEFAULT_ACCOUNT_ID, destination);
            return;
          }
          // all good - respond success
          respondSuccess(message.getChannel(), request);
          helper.finish(Success);
          break;
        }

        case META: {
          LOG.trace("Received a request for stream meta data," +
              " which is not implemented yet.");
          helper.finish(BadRequest);
          respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
          return;
        }

        // GET means client wants to view the content of a queue.
        // 1. obtain a consumerId with GET stream?q=newConsumer
        // 2. dequeue an event with GET stream?q=dequeue with the consumerId as
        //    an HTTP header
        case NEWID: {
          String queueURI = FlowStream.buildStreamURI(
              Constants.defaultAccount, destination).toString();
          QueueAdmin.GetGroupID op =
              new QueueAdmin.GetGroupID(queueURI.getBytes());
          long id;
          try {
            id = this.collector.getExecutor().
                execute(OperationContext.DEFAULT, op);
          } catch (Exception e) {
            LOG.error("Exception for GetGroupID: " + e.getMessage(), e);
            helper.finish(Error);
            this.collector.getStreamCache().refreshStream(
                OperationContext.DEFAULT_ACCOUNT_ID, destination);
            respondError(message.getChannel(),
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
            break;
          }
          byte[] responseBody = Long.toString(id).getBytes();
          Map<String, String> headers = Maps.newHashMap();
          headers.put(Constants.HEADER_STREAM_CONSUMER, Long.toString(id));

          respond(message.getChannel(), request,
              HttpResponseStatus.CREATED, headers, responseBody);
          helper.finish(Success);
          break;
        }

        case DEQUEUE: {
          // there must be a header with the consumer id in the request
          String idHeader = request.getHeader(Constants.HEADER_STREAM_CONSUMER);
          Long id = null;
          if (idHeader == null) {
            LOG.trace("Received a dequeue request without header " +
                Constants.HEADER_STREAM_CONSUMER);
          } else {
            try {
              id = Long.valueOf(idHeader);
            } catch (NumberFormatException e) {
              LOG.trace("Received a dequeue request with a invalid header "
                  + Constants.HEADER_STREAM_CONSUMER + ": " + e.getMessage());
            }
          }
          if (null == id) {
            helper.finish(BadRequest);
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
          // valid consumer id, dequeue and return it
          String queueURI = FlowStream.buildStreamURI(
              Constants.defaultAccount, destination).toString();
          // 0th instance of group 'id' of size 1
          QueueConsumer queueConsumer = new QueueConsumer(0, id, 1);
          // singleEntry = true means we must ack before we can see the next entry
          QueueConfig queueConfig =
              new QueueConfig(QueuePartitioner.PartitionerType.RANDOM, true);
          QueueDequeue dequeue = new QueueDequeue(
              queueURI.getBytes(), queueConsumer, queueConfig);
          DequeueResult result;
          try {
            result = this.collector.getExecutor().
                execute(OperationContext.DEFAULT, dequeue);
          } catch (OperationException e) {
            helper.finish(Error);
            LOG.error("Error dequeueing from stream " + queueURI +
                " with consumer " + queueConsumer + ": " + e.getMessage(), e);
            respondError(message.getChannel(),
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
            // refresh the cache for this stream, it may have been deleted
            this.collector.getStreamCache().refreshStream(
                OperationContext.DEFAULT_ACCOUNT_ID, destination);
            return;
          }
          if (result.isEmpty()) {
            respondSuccess(message.getChannel(), request,
                HttpResponseStatus.NO_CONTENT);
            helper.finish(NoData);
            return;
          }
          // try to deserialize into an event (tuple)
          Map<String, String> headers;
          byte[] body;
          try {
            TupleSerializer serializer = new TupleSerializer(false);
            Tuple tuple = serializer.deserialize(result.getValue());
            headers = tuple.get("headers");
            body = tuple.get("body");
          } catch (Exception e) {
            LOG.error("Exception when deserializing data from stream "
                + queueURI + " into an event: " + e.getMessage());
            helper.finish(Error);
            respondError(message.getChannel(),
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
          }
          // ack the entry so that the next request can see the next entry
          QueueAck ack = new QueueAck(
              queueURI.getBytes(), result.getEntryPointer(), queueConsumer);
          try {
            this.collector.getExecutor().
                execute(OperationContext.DEFAULT, ack);
          } catch (Exception e) {
            LOG.error("Ack failed to for queue " + queueURI + ", consumer "
                + queueConsumer + " and pointer " + result.getEntryPointer() +
                ": " + e.getMessage(), e);
            helper.finish(Error);
            respondError(message.getChannel(),
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
          }

          // prefix each header with the destination to distinguish them from
          // HTTP headers
          Map<String, String> prefixedHeaders = Maps.newHashMap();
          for (Map.Entry<String, String> header : headers.entrySet())
            prefixedHeaders.put(destination + "." + header.getKey(),
                header.getValue());
          // now the headers and body are ready to be sent back
          respond(message.getChannel(), request,
              HttpResponseStatus.OK, prefixedHeaders, body);
          helper.finish(Success);
          break;
        }
        default: {
          // this should not happen because we checked above -> internal error
          helper.finish(Error);
          respondError(message.getChannel(),
              HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception caught for connector '" +
          this.collector.getName() + "'. ", e.getCause());
      helper.finish(Error);
      if (message.getChannel().isOpen()) {
        respondError(message.getChannel(),
            HttpResponseStatus.INTERNAL_SERVER_ERROR);
        message.getChannel().close();
      }
    }

  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
    MetricsHelper.meterError(metrics, this.collector.getName());
    LOG.error("Exception caught for collector '" +
        this.collector.getName() + "'. ", e.getCause());
    e.getChannel().close();
  }
}
