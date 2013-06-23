package com.continuuity.gateway.collector;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.Id;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueAck;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.GetQueueInfo;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data.operation.ttqueue.admin.QueueInfo;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.GatewayMetricsHelperWrapper;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.internal.app.verification.StreamVerification;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import com.continuuity.streamevent.DefaultStreamEvent;
import com.continuuity.streamevent.StreamEventCodec;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the rest collector. This supports
 * POST requests to send an event to a stream, and GET requests to inspect
 * or retrieve events from the stream.
 */
public class RestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
    .getLogger(RestHandler.class);

  /**
   * The allowed methods for this handler.
   */
  Set<HttpMethod> allowedMethods = Sets.newHashSet(
    HttpMethod.PUT,
    HttpMethod.POST,
    HttpMethod.GET);

  /**
   * The collector that created this handler. It has collector name and consumer
   */
  private RestCollector collector;

  /**
   * The metrics object of the rest accessor.
   */
  private CMetrics metrics;

  /**
   * All the paths have to be of the form.
   * http://host:port&lt;pathPrefix>&lt;stream>
   * For instance, if config(prefix="/v0.1/" path="stream/"),
   * then pathPrefix will be "/v0.1/stream/",
   * and a valid request is POST http://host:port/v0.1/stream/mystream
   */
  private String pathPrefix;

  /**
   * Disallow default constructor.
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
    if (name.startsWith(destinationPrefix)) {
      return name.substring(destinationPrefix.length());
    }
    return null;
  }

  private static final int UNKNOWN = 0;
  private static final int ENQUEUE = 1;
  private static final int NEWID = 2;
  private static final int DEQUEUE = 3;
  private static final int INFO = 4;
  private static final int PING = 5;
  private static final int CREATE = 6;

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {

    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();

    LOG.trace("Request received: " + method + " " + requestUri);
    GatewayMetricsHelperWrapper helper = new GatewayMetricsHelperWrapper(new MetricsHelper(
      this.getClass(), this.metrics, this.collector.getMetricsQualifier()), collector.getGatewayMetrics());

    try {

      // we only support POST and GET
      if (method != HttpMethod.PUT && method != HttpMethod.POST && method != HttpMethod.GET) {
        LOG.trace("Received a " + method + " request, which is not supported");
        helper.finish(BadRequest);
        respondNotAllowed(message.getChannel(), allowedMethods);
        return;
      }

      // we do not support a query or parameters in the URL
      QueryStringDecoder decoder = new QueryStringDecoder(requestUri);
      Map<String, List<String>> parameters = decoder.getParameters();

      // if authentication is enabled, verify an authentication token has been
      // passed and then verify the token is valid
      if (!collector.getAuthenticator().authenticateRequest(request)) {
        LOG.info("Received an unauthorized request");
        respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
        helper.finish(BadRequest);
        return;
      }

      String accountId = collector.getAuthenticator().getAccountId(request);
      if (accountId == null || accountId.isEmpty()) {
        LOG.info("No valid account information found");
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        helper.finish(NotFound);
        return;
      }

      int operation = UNKNOWN;
      if (method == HttpMethod.PUT) {
        operation = CREATE;
        helper.setMethod("create");
      } else if (method == HttpMethod.POST) {
        operation = ENQUEUE;
        helper.setMethod("enqueue");
      } else if (method == HttpMethod.GET) {
        if ("/ping".equals(requestUri)) {
          operation = PING;
          helper.setMethod("ping");
        } else if (parameters == null || parameters.size() == 0) {
          operation = INFO;
          helper.setMethod("getQueueInfo");
        } else {
          List<String> qParams = parameters.get("q");
          if (qParams != null && qParams.size() == 1) {
            if ("newConsumer".equals(qParams.get(0))) {
              operation = NEWID;
              helper.setMethod("getNewId");
            } else if ("dequeue".equals(qParams.get(0))) {
              operation = DEQUEUE;
              helper.setMethod("dequeue");
            } else if ("info".equals(qParams.get(0))) {
              operation = INFO;
              helper.setMethod("info");
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

      if ((operation == ENQUEUE || operation == INFO) &&
        parameters != null && !parameters.isEmpty()) {
        helper.finish(BadRequest);
        LOG.trace(
          "Received a request with query parameters, which is not supported");
        respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
        return;
      }

      // is this a ping? (http://gw:port/ping) if so respond OK and done
      if (PING == operation) {
        respondToPing(message.getChannel(), request);
        helper.finish(Success);
        return;
      }

      // does the path of the URL start with the correct prefix, and is it of
      // the form <flowname> or <flowname</<streamname> after that? Otherwise
      // we will not accept this request.
      String destination = null;
      String path = decoder.getPath();
      if (path.startsWith(this.pathPrefix)) {
        String resourceName = path.substring(this.pathPrefix.length());
        if (resourceName.startsWith("/")) {
          resourceName = resourceName.substring(1);
        }
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

      // validate the existence of the stream except for create stream operation
      if (operation != CREATE && !this.collector.getStreamCache().validateStream(accountId, destination)) {
        helper.finish(NotFound);
        LOG.trace("Received a request for non-existent stream " + destination);
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }

      OperationContext operationContext = new OperationContext(accountId);
      switch (operation) {
        case ENQUEUE: {
          // build a new event from the request, start with the headers
          Map<String, String> headers = Maps.newHashMap();
          // set some built-in headers
          headers.put(Constants.HEADER_FROM_COLLECTOR, this.collector.getName());
          headers.put(Constants.HEADER_DESTINATION_STREAM, destination);
          // and transfer all other headers that are to be preserved
          String prefix = destination + ".";
          for (String header : request.getHeaderNames()) {
            String preservedHeader = isPreservedHeader(prefix, header);
            if (preservedHeader != null) {
              headers.put(preservedHeader, request.getHeader(header));
            }
          }
          // read the body of the request
          ByteBuffer body = request.getContent().toByteBuffer();
          // and create a stream event
          StreamEvent event = new DefaultStreamEvent(headers, body);

          LOG.trace("Sending event to consumer: " + event);
          // let the consumer process the event.
          // in case of exception, respond with internal error
          try {
            this.collector.getConsumer().consumeEvent(event, accountId);
          } catch (Exception e) {
            LOG.error("Error consuming single event: " + e.getMessage());
            helper.finish(Error);
            respondError(message.getChannel(),
                         HttpResponseStatus.INTERNAL_SERVER_ERROR);
            // refresh cache for this stream - apparently it has disappeared
            this.collector.getStreamCache().refreshStream(accountId, destination);
            return;
          }
          // all good - respond success
          respondSuccess(message.getChannel(), request);
          helper.finish(Success);
          break;
        }

        case INFO: {

          String queueURI = QueueName.fromStream(new Id.Account(accountId), destination)
            .toString();
          GetQueueInfo getInfo = new GetQueueInfo(queueURI.getBytes());
          OperationResult<QueueInfo> info = null;
          boolean noEventsInDF = false;

          try {

            info = this.collector.getExecutor().
              execute(operationContext, getInfo);
            noEventsInDF = info.isEmpty()
              || info.getValue().getJSONString() == null;
          } catch (Exception e) {
            if (e instanceof OperationException) {
              OperationException oe = (OperationException) e;
              noEventsInDF = oe.getStatus() == StatusCode.QUEUE_NOT_FOUND;
            }
            if (!noEventsInDF) {
              LOG.error("Exception for GetQueueInfo: " + e.getMessage(), e);
              helper.finish(Error);
              respondError(message.getChannel(),
                           HttpResponseStatus.INTERNAL_SERVER_ERROR);
              break;
            }
          }
          if (noEventsInDF) {
            MetadataService mds = this.collector.getMetadataService();
            //Check if a stream exists
            Stream existingStream = mds.getStream(new Account(accountId), new Stream(destination));
            boolean existsInMDS = existingStream.isExists();

            if (existsInMDS) {
              byte[] responseBody = "{}".getBytes();
              Map<String, String> headers = Maps.newHashMap();
              headers.put(HttpHeaders.Names.CONTENT_TYPE, "application/json");
              respond(message.getChannel(), request,
                      HttpResponseStatus.OK, headers, responseBody);
              helper.finish(Success);
            } else {
              respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
              helper.finish(NotFound);
            }
            this.collector.getStreamCache().refreshStream(accountId, destination);
          } else {
            byte[] responseBody = info.getValue().getJSONString().getBytes();
            Map<String, String> headers = Maps.newHashMap();
            headers.put(HttpHeaders.Names.CONTENT_TYPE, "application/json");
            respond(message.getChannel(), request,
                    HttpResponseStatus.OK, headers, responseBody);
            helper.finish(Success);
          }
          break;
        }

        // GET means client wants to view the content of a queue.
        // 1. obtain a consumerId with GET stream?q=newConsumer
        // 2. dequeue an event with GET stream?q=dequeue with the consumerId as
        //    an HTTP header
        case NEWID: {
          String queueURI = QueueName.fromStream(new Id.Account(accountId), destination)
            .toString();
          GetGroupID op =
            new GetGroupID(queueURI.getBytes());
          long id;
          try {
            id = this.collector.getExecutor().
              execute(operationContext, op);
            // Configure queue. A consumer is required to call getGroupId before dequeue-ing, we can configure the queue
            // during the getGroupId call since getGroupId is called once per consumer.
            // Also, if any changes are made to queue config or queue consumer below then same change needs to be
            // made in dequeue too.
            QueueConfig queueConfig = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true);
            QueueConsumer queueConsumer = new QueueConsumer(0, id, 1, queueConfig);
            this.collector.getExecutor()
              .execute(operationContext, new QueueConfigure(queueURI.getBytes(), queueConsumer));
          } catch (Exception e) {
            LOG.error("Exception for GetGroupID: " + e.getMessage(), e);
            helper.finish(Error);
            this.collector.getStreamCache().refreshStream(accountId, destination);
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
          String queueURI = QueueName.fromStream(new Id.Account(accountId), destination)
            .toString();
          // 0th instance of group 'id' of size 1
          // NOTE: the queue is configured during getGroupId call, if any changes are made to the queue config
          // or queue consumer below then same change needs to be made for configure call too.
          QueueConfig queueConfig = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true);
          QueueConsumer queueConsumer = new QueueConsumer(0, id, 1, queueConfig);
          // singleEntry = true means we must ack before we can see the next entry
          QueueDequeue dequeue = new QueueDequeue(queueURI.getBytes(), queueConsumer, queueConfig);
          DequeueResult result;
          try {
            result = this.collector.getExecutor().
              execute(operationContext, dequeue);
          } catch (OperationException e) {
            helper.finish(Error);
            LOG.error("Error dequeueing from stream " + queueURI +
                        " with consumer " + queueConsumer + ": " + e.getMessage(), e);
            respondError(message.getChannel(),
                         HttpResponseStatus.INTERNAL_SERVER_ERROR);
            // refresh the cache for this stream, it may have been deleted
            this.collector.getStreamCache().refreshStream(accountId, destination);
            return;
          }
          if (result.isEmpty() || result.getEntry().getData() == null) {
            respondSuccess(message.getChannel(), request,
                           HttpResponseStatus.NO_CONTENT);
            helper.finish(NoData);
            return;
          }
          // try to deserialize into an event
          Map<String, String> headers;
          byte[] body;
          try {
            StreamEventCodec deserializer = new StreamEventCodec();
            StreamEvent event = deserializer.decodePayload(result.getEntry().getData());
            headers = event.getHeaders();
            body = Bytes.toBytes(event.getBody());
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
              commit(operationContext, ack);
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
          for (Map.Entry<String, String> header : headers.entrySet()) {
            prefixedHeaders.put(destination + "." + header.getKey(),
                                header.getValue());
          }
          // now the headers and body are ready to be sent back
          respond(message.getChannel(), request,
                  HttpResponseStatus.OK, prefixedHeaders, body);
          helper.finish(Success);
          break;
        }
        case CREATE: {
          //our user interfaces do not support stream name
          //we are using id for stream name until it is supported in UI's
          String streamId = destination;

          if (!isId(streamId)) {
            LOG.info("Stream id '{}' is not a printable ascii character string", destination);
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            helper.finish(BadRequest);
            return;
          }

          MetadataService mds = collector.getMetadataService();

          Account account = new Account(accountId);
          Stream stream = new Stream(destination);
          stream.setName(streamId); //

          //Check if a stream with the same id exists
          Stream existingStream = mds.getStream(account, stream);
          if (!existingStream.isExists()) {
            mds.createStream(account, stream);
          }

          respondSuccess(message.getChannel(), request);
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
    MetricsHelper.meterError(metrics, this.collector.getMetricsQualifier());
    LOG.error("Exception caught for collector '" +
                this.collector.getName() + "'. ", e.getCause());
    e.getChannel().close();
  }

  private boolean isId(String id) {
    StreamSpecification spec = new StreamSpecification.Builder().setName(id).create();
    StreamVerification verifier = new StreamVerification();
    VerifyResult result = verifier.verify(spec);
    return (result.getStatus() == VerifyResult.Status.SUCCESS);
  }
}
