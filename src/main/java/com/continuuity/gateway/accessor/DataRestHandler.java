package com.continuuity.gateway.accessor;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.gateway.util.Util;
import com.continuuity.common.metrics.CMetrics;
import com.google.common.collect.Lists;
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

import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * This is the http request handler for the rest accessor. At this time it
 * only accepts GET requests to retrieve a value for a key from a named table.
 */
public class DataRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(DataRestHandler.class);

  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = {
      HttpMethod.GET,
      HttpMethod.DELETE,
      HttpMethod.PUT,
      HttpMethod.POST
  };

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private DataRestAccessor accessor;

  /**
   * The metrics object of the rest accessor
   */
  private CMetrics metrics;

  /**
   * All the paths have to be of the form
   * http://host:port&lt;pathPrefix>&lt;table>/&lt;key>
   * For instance, if config(prefix="/v0.1/" path="table/"),
   * then pathPrefix will be "/v0.1/table/", and a valid request is
   * GET http://host:port/v0.1/table/mytable/12345678
   */
  private String pathPrefix;

  /**
   * Constructor requires the accessor that created this
   *
   * @param accessor the accessor that created this
   */
  DataRestHandler(DataRestAccessor accessor) {
    this.accessor = accessor;
    this.metrics = accessor.getMetricsClient();
    this.pathPrefix =
        accessor.getHttpConfig().getPathPrefix() +
        accessor.getHttpConfig().getPathMiddle();
  }

  private static final int BAD = -1;
  private static final int UNKNOWN = 0;
  private static final int READ = 1;
  private static final int WRITE = 2;
  private static final int DELETE = 3;
  private static final int LIST = 4;
  private static final int CLEAR = 5;

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {
    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();

    LOG.trace("Request received: " + method + " " + requestUri);
    metrics.meter(this.getClass(), Constants.METRIC_REQUESTS, 1);

    // we only support get requests for now
    if (method != HttpMethod.GET && method != HttpMethod.DELETE &&
        method != HttpMethod.PUT && method != HttpMethod.POST) {
      LOG.trace("Received a " + method + " request, which is not supported");
      respondNotAllowed(message.getChannel(), allowedMethods);
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      return;
    }

    // based on the request URL, determine what to do
    QueryStringDecoder decoder = new QueryStringDecoder(requestUri);
    Map<String, List<String>> parameters = decoder.getParameters();
    List<String> clearParams = null;
    int operation = UNKNOWN;
    if (method == HttpMethod.PUT)
      operation = WRITE;
    else if (method == HttpMethod.DELETE)
      operation = DELETE;
    else if (method == HttpMethod.POST) {
      clearParams = parameters.get("clear");
      if (clearParams != null && clearParams.size() > 0)
        operation = CLEAR;
      else
        operation = BAD;
    } else if (method == HttpMethod.GET) {
      if (parameters == null || parameters.size() == 0)
        operation = READ;
      else {
        List<String> qParams = parameters.get("q");
        if (qParams != null && qParams.size() == 1
            && "list".equals(qParams.get(0)))
          operation = LIST;
        else
          operation = BAD;
    } }

    // respond with error for bad requests
    if (operation == BAD) {
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      LOG.trace("Received an incomplete request '" + request.getUri() + "'.");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      return;
    }
    // respond with error for unknown requests
    if (operation == UNKNOWN) {
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      LOG.trace("Received an unsupported " + method + " request '"
          + request.getUri() + "'.");
      respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
      return;
    }

    // respond with error for parameters if the operation does not allow them
    if (operation != LIST && operation != CLEAR &&
        parameters != null && !parameters.isEmpty()) {
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      LOG.trace("Received a " + method +
          " request with query parameters, which is not supported");
      respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
      return;
    }

    // we only support requests of the form
    // POST http://host:port/prefix/path/<tablename>/<key>
    String destination = null, key = null;
    String path = decoder.getPath();
    if (path.startsWith(this.pathPrefix)) {
      String remainder = path.substring(this.pathPrefix.length());
      int pos = remainder.indexOf("/");
      if (pos < 0) {
        destination = remainder.length() == 0 ? null : remainder;
        key = null;
      } else {
        destination = remainder.substring(0, pos);
        // no further / is allowed in the path
        if (remainder.length() == pos + 1) {
          key = null;
        } else if (remainder.indexOf('/', pos + 1) < 0)
          key = remainder.substring(pos + 1);
        else {
          metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
          LOG.trace("Received a request with invalid path " +
              path + "(path does not end with key)");
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
          return;
    } } }

    // check that URL could be parsed up to destination
    // except for CLEAR, where no destination may be given
    if ((destination == null && operation != CLEAR) ||
        (destination != null && operation == CLEAR)) {
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      LOG.trace("Received a request with unknown path '" + path + "'.");
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      return;
    }

    // all operations except for LIST and CLEAR need a key
    if (operation != LIST && operation != CLEAR &&
        (key == null || key.length() == 0)) {
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      LOG.trace("Received a request with invalid path " +
          path + "(no key given)");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      return;
    }
    // operation LIST and CLEAR must not have a key
    if ((operation == LIST || operation == CLEAR) &&
        (key != null && key.length() > 0)) {
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      LOG.trace("Received a request with invalid path " +
          path + "(no key may be given)");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      return;
    }

    // check that destination is valid - for now only "default" is allowed
    if (destination != null && !"default".equals(destination)) {
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      LOG.trace("Received a request with path " + path +
          " for destination other than 'default'");
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      return;
    }

    // key is URL-encoded, decode it
    byte[] keyBinary = null;
    if (key != null) {
      key = URLDecoder.decode(key, "ISO8859_1");
      LOG.trace("Received " + method + " request for key '" + key + "'.");
      keyBinary = key.getBytes("ISO8859_1");
    }

    switch(operation) {
      case READ : {
        metrics.counter(this.getClass(), Constants.METRIC_READ_REQUESTS, 1);
        // Get the value from the data fabric
        OperationResult<byte[]> result;
        try {
          ReadKey read = new ReadKey(keyBinary);
          result = this.accessor.getExecutor().
              execute(OperationContext.DEFAULT, read);
        } catch (Exception e) {
          metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
          LOG.error("Error during ReadKey: " + e.getMessage(), e);
          respondError(message.getChannel(),
              HttpResponseStatus.INTERNAL_SERVER_ERROR);
          return;
        }
        if (result.isEmpty()) {
          metrics.counter(this.getClass(), Constants.METRIC_NOT_FOUND, 1);
          respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        } else {
          metrics.meter(this.getClass(), Constants.METRIC_SUCCESS, 1);
          respondSuccess(message.getChannel(), request, result.getValue());
        }
        break;
      }
      case LIST : {
        metrics.counter(this.getClass(), Constants.METRIC_LIST_REQUESTS, 1);
        int start = 0, limit = 100;
        String enc = "url";
        List<String> startParams = parameters.get("start");
        if (startParams != null && !startParams.isEmpty()) {
          try {
            start = Integer.valueOf(startParams.get(0));
          } catch (NumberFormatException e) {
            metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
            LOG.trace("Received a request with invalid start '" +
                startParams.get(0) + "' (not an integer).");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
        }
        List<String> limitParams = parameters.get("limit");
        if (limitParams != null && !limitParams.isEmpty()) {
          try {
            limit = Integer.valueOf(limitParams.get(0));
          } catch (NumberFormatException e) {
            metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
            LOG.trace("Received a request with invalid limit '" +
                limitParams.get(0) + "' (not an integer).");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
        }
        List<String> encParams = parameters.get("enc");
        if (encParams != null && !encParams.isEmpty()) {
          enc = encParams.get(0);
          if (!"hex".equals(enc) && !"url".equals(enc) &&
              !Charset.isSupported(enc)) {
            metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
            LOG.trace("Received a request with invalid encoding " + enc + ".");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
        }
        OperationResult<List<byte[]>> result;
        try {
          ReadAllKeys read = new ReadAllKeys(start, limit);
          result = this.accessor.getExecutor().
              execute(OperationContext.DEFAULT, read);
        } catch (Exception e) {
          metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
          LOG.error("Error listing keys: " + e.getMessage() + ".", e);
          respondError(message.getChannel(),
              HttpResponseStatus.INTERNAL_SERVER_ERROR);
          return;
        }
        StringBuilder builder = new StringBuilder();
        if (!result.isEmpty()) {
          for (byte[] keyBytes : result.getValue()) {
            builder.append(Util.encode(keyBytes, enc));
            builder.append('\n');
          }
        }
        // for hex or url, send it back as ASCII, otherwise use encoding
        byte[] responseBody = builder.toString().getBytes(
            "url".equals(enc) || "hex".equals(enc) ? "ASCII" : enc);
        metrics.meter(this.getClass(), Constants.METRIC_SUCCESS, 1);
        respondSuccess(message.getChannel(), request, responseBody);
        break;
      }

      case DELETE : {
        metrics.counter(this.getClass(), Constants.METRIC_DELETE_REQUESTS, 1);
        // first perform a Read to determine whether the key exists
        try {
          ReadKey read = new ReadKey(keyBinary);
          OperationResult<byte[]> result =
              this.accessor.getExecutor().
                  execute(OperationContext.DEFAULT, read);
          if (result.isEmpty()) {
            // key does not exist -> Not Found
            metrics.counter(this.getClass(), Constants.METRIC_NOT_FOUND, 1);
            respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
            return;
          }
        } catch (Exception e) {
          // something went wrong, internal error
          metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
          LOG.error("Error during ReadKey: " + e.getMessage(), e);
          respondError(message.getChannel(),
              HttpResponseStatus.INTERNAL_SERVER_ERROR);
          break;
        }

        // now that we know the key exists, delete it
        try {
          Delete delete = new Delete(keyBinary);
          this.accessor.getExecutor().
              execute(OperationContext.DEFAULT, delete);
          // deleted successfully
          metrics.meter(this.getClass(), Constants.METRIC_SUCCESS, 1);
          respondSuccess(message.getChannel(), request);
        } catch (Exception e) {
          // something went wrong, internal error
          metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
          LOG.error("Error during Delete: " + e.getMessage(), e);
          respondError(message.getChannel(),
              HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
        break;
      }
      case WRITE : {
        metrics.meter(this.getClass(), Constants.METRIC_WRITE_REQUESTS, 1);
        // read the body of the request and add it to the event
        ChannelBuffer content = request.getContent();
        if (content == null) {
          // PUT without content -> 400 Bad Request
          metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
          return;
        }
        int length = content.readableBytes();
        byte[] bytes = new byte[length];
        content.readBytes(bytes);
        // create a write and attempt to execute it
        Write write = new Write(keyBinary, bytes);
        try {
          this.accessor.getExecutor().
              execute(OperationContext.DEFAULT, write);
          // written successfully
          metrics.meter(this.getClass(), Constants.METRIC_SUCCESS, 1);
          respondSuccess(message.getChannel(), request);
        } catch (Exception e) {
          // something went wrong, internal error
          metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
          LOG.error("Error during Write: " + e.getMessage(), e);
          respondError(message.getChannel(),
              HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
        break;
      }
      case CLEAR : {
        metrics.meter(this.getClass(), Constants.METRIC_CLEAR_REQUESTS, 1);
        // figure out what to clear
        List<ClearFabric.ToClear> toClear = Lists.newArrayList();
        for (String param : clearParams) {
          for (String what : param.split(",")) {
            if ("all".equals(what))
              toClear.add(ClearFabric.ToClear.ALL);
            else if ("data".equals(what))
              toClear.add(ClearFabric.ToClear.DATA);
            else if ("meta".equals(what))
              toClear.add(ClearFabric.ToClear.META);
            else if ("tables".equals(what))
              toClear.add(ClearFabric.ToClear.TABLES);
            else if ("queues".equals(what))
              toClear.add(ClearFabric.ToClear.QUEUES);
            else if ("streams".equals(what))
              toClear.add(ClearFabric.ToClear.STREAMS);
            else {
              metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
              LOG.trace("Received invalid clear request with URI " +
                  requestUri);
              respondError(message.getChannel(),
                  HttpResponseStatus.BAD_REQUEST);
              break;
        } } }
        ClearFabric clearFabric = new ClearFabric(toClear);
        try {
          this.accessor.getExecutor().
              execute(OperationContext.DEFAULT, clearFabric);
        } catch (Exception e) {
          LOG.error("Exception clearing data fabric: ", e);
          metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
          respondError(message.getChannel(),
              HttpResponseStatus.INTERNAL_SERVER_ERROR);
          break;
        }
        metrics.meter(this.getClass(), Constants.METRIC_SUCCESS, 1);
        respondSuccess(message.getChannel(), request);
        break;
      }

      default: {
        // this should not happen because we checked above -> internal error
        metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
        respondError(message.getChannel(),
            HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
    metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
    LOG.error("Exception caught for connector '" +
        this.accessor.getName() + "'. ", e.getCause());
    respondError(ctx.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
    e.getChannel().close();
  }
}
