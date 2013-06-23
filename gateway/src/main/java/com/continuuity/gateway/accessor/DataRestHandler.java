package com.continuuity.gateway.accessor;

import com.continuuity.api.data.OperationResult;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.data.operation.ClearFabric;
import com.continuuity.data.operation.Delete;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.Read;
import com.continuuity.data.operation.ReadAllKeys;
import com.continuuity.data.operation.Write;
import com.continuuity.gateway.GatewayMetricsHelperWrapper;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.gateway.util.Util;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.Set;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the rest accessor. At this time it
 * only accepts GET requests to retrieve a value for a key from a named table.
 */
public class DataRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
    .getLogger(DataRestHandler.class);

  /**
   * The allowed methods for this handler.
   */
  Set<HttpMethod> allowedMethods = Sets.newHashSet(
    HttpMethod.GET,
    HttpMethod.DELETE,
    HttpMethod.PUT,
    HttpMethod.POST);

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private DataRestAccessor accessor;

  /**
   * The metrics object of the rest accessor.
   */
  private CMetrics metrics;

  /**
   * All the paths have to be of the form
   * http://host:port&lt;pathPrefix>&lt;table>/&lt;key>.
   * For instance, if config(prefix="/v0.1/" path="table/"),
   * then pathPrefix will be "/v0.1/table/", and a valid request is
   * GET http://host:port/v0.1/table/mytable/12345678
   */
  private String pathPrefix;

  /**
   * Constructor requires the accessor that created this.
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
  private static final int PING = 6;

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {
    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();

    LOG.trace("Request received: " + method + " " + requestUri);
    GatewayMetricsHelperWrapper helper = new GatewayMetricsHelperWrapper(new MetricsHelper(
      this.getClass(), this.metrics, this.accessor.getMetricsQualifier()), accessor.getGatewayMetrics());

    try {
      // check whether the request's HTTP method is supported
      if (!allowedMethods.contains(method)) {
        LOG.trace("Received a " + method + " request, which is not supported");
        respondNotAllowed(message.getChannel(), allowedMethods);
        helper.finish(BadRequest);
        return;
      }

      // based on the request URL, determine what to do
      QueryStringDecoder decoder = new QueryStringDecoder(requestUri);
      Map<String, List<String>> parameters = decoder.getParameters();
      List<String> clearParams = null;
      int operation = UNKNOWN;

      // if authentication is enabled, verify an authentication token has been
      // passed and then verify the token is valid
      if (!accessor.getAuthenticator().authenticateRequest(request)) {
        respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
        helper.finish(BadRequest);
        return;
      }

      String accountId = accessor.getAuthenticator().getAccountId(request);
      if (accountId == null || accountId.isEmpty()) {
        LOG.info("No valid account information found");
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        helper.finish(NotFound);
        return;
      }

      if (method == HttpMethod.PUT) {
        operation = WRITE;
        helper.setMethod("write");
      } else if (method == HttpMethod.DELETE) {
        operation = DELETE;
        helper.setMethod("delete");
      } else if (method == HttpMethod.POST) {
        clearParams = parameters.get("clear");
        if (clearParams != null && clearParams.size() > 0) {
          operation = CLEAR;
          helper.setMethod("clear");
        } else {
          operation = BAD;
        }
      } else if (method == HttpMethod.GET) {
        if ("/ping".equals(requestUri)) {
          operation = PING;
          helper.setMethod("ping");
        } else if (parameters == null || parameters.size() == 0) {
          operation = READ;
          helper.setMethod("read");
        } else {
          List<String> qParams = parameters.get("q");
          if (qParams != null && qParams.size() == 1
            && "list".equals(qParams.get(0))) {
            operation = LIST;
            helper.setMethod("list");
          } else {
            operation = BAD;
          }
        }
      }

      // respond with error for bad requests
      if (operation == BAD) {
        helper.finish(BadRequest);
        LOG.trace("Received an incomplete request '" + request.getUri() + "'.");
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }
      // respond with error for unknown requests
      if (operation == UNKNOWN) {
        helper.finish(BadRequest);
        LOG.trace("Received an unsupported " + method + " request '"
                    + request.getUri() + "'.");
        respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
        return;
      }

      // respond with error for parameters if the operation does not allow them
      if (operation != LIST && operation != CLEAR &&
        parameters != null && !parameters.isEmpty()) {
        helper.finish(BadRequest);
        LOG.trace("Received a " + method +
                    " request with query parameters, which is not supported");
        respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
        return;
      }

      // is this a ping? (http://gw:port/ping) if so respond OK and done
      if (PING == operation) {
        respondToPing(message.getChannel(), request);
        helper.finish(Success);
        return;
      }

      String destination = null, key = null;
      String path = decoder.getPath();

      // we only support requests of the form
      // http://host:port/prefix/path/<tablename>/<key>
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
          } else if (remainder.indexOf('/', pos + 1) < 0) {
            key = remainder.substring(pos + 1);
          } else {
            helper.finish(BadRequest);
            LOG.trace("Received a request with invalid path " +
                        path + "(path does not end with key)");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
        }
      }

      // check that URL could be parsed up to destination
      // except for CLEAR, where no destination may be given
      if ((destination == null && operation != CLEAR) ||
        (destination != null && operation == CLEAR)) {
        helper.finish(NotFound);
        LOG.trace("Received a request with unknown path '" + path + "'.");
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }

      // all operations except for LIST and CLEAR need a key
      if (operation != LIST && operation != CLEAR &&
        (key == null || key.length() == 0)) {
        helper.finish(BadRequest);
        LOG.trace("Received a request with invalid path " +
                    path + "(no key given)");
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }
      // operation LIST and CLEAR must not have a key
      if ((operation == LIST || operation == CLEAR) &&
        (key != null && key.length() > 0)) {
        helper.finish(BadRequest);
        LOG.trace("Received a request with invalid path " +
                    path + "(no key may be given)");
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }

      String table = "default".equals(destination) ? null : destination;

      // key is URL-encoded, decode it
      byte[] keyBinary = null;
      if (key != null) {
        key = URLDecoder.decode(key, "ISO8859_1");
        LOG.trace("Received " + method + " request for key '" + key + "'.");
        keyBinary = key.getBytes("ISO8859_1");
      }

      OperationContext operationContext = new OperationContext(accountId);
      switch (operation) {
        case READ: {
          // Get the value from the data fabric
          OperationResult<Map<byte[], byte[]>> result;
          try {
            Read read = new Read(table, keyBinary, Operation.KV_COL);
            result = this.accessor.getExecutor().
              execute(operationContext, read);
          } catch (Exception e) {
            helper.finish(Error);
            LOG.error("Error during Read: " + e.getMessage(), e);
            respondError(message.getChannel(),
                         HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
          }
          if (result.isEmpty() || null == result.getValue().get(Operation.KV_COL)) {
            respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
            helper.finish(NoData);
          } else {
            respondSuccess(message.getChannel(), request, result.getValue().get(Operation.KV_COL));
            helper.finish(Success);
          }
          break;
        }
        case LIST: {
          int start = 0, limit = 100;
          String enc = "url";
          List<String> startParams = parameters.get("start");
          if (startParams != null && !startParams.isEmpty()) {
            try {
              start = Integer.parseInt(startParams.get(0));
            } catch (NumberFormatException e) {
              helper.finish(BadRequest);
              LOG.trace("Received a request with invalid start '" +
                          startParams.get(0) + "' (not an integer).");
              respondError(message.getChannel(),
                           HttpResponseStatus.BAD_REQUEST);
              return;
            }
          }
          List<String> limitParams = parameters.get("limit");
          if (limitParams != null && !limitParams.isEmpty()) {
            try {
              limit = Integer.parseInt(limitParams.get(0));
            } catch (NumberFormatException e) {
              helper.finish(BadRequest);
              LOG.trace("Received a request with invalid limit '" +
                          limitParams.get(0) + "' (not an integer).");
              respondError(message.getChannel(),
                           HttpResponseStatus.BAD_REQUEST);
              return;
            }
          }
          List<String> encParams = parameters.get("enc");
          if (encParams != null && !encParams.isEmpty()) {
            enc = encParams.get(0);
            if (!"hex".equals(enc) && !"url".equals(enc) &&
              !Charset.isSupported(enc)) {
              helper.finish(BadRequest);
              LOG.trace("Received a request with invalid encoding "
                          + enc + ".");
              respondError(message.getChannel(),
                           HttpResponseStatus.BAD_REQUEST);
              return;
            }
          }
          OperationResult<List<byte[]>> result;
          try {
            ReadAllKeys read = new ReadAllKeys(table, start, limit);
            result = this.accessor.getExecutor().
              execute(operationContext, read);
          } catch (Exception e) {
            helper.finish(Error);
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
          respondSuccess(message.getChannel(), request, responseBody);
          helper.finish(Success);
          break;
        }

        case DELETE: {
          // first perform a Read to determine whether the key exists
          try {
            Read read = new Read(table, keyBinary, Operation.KV_COL);
            OperationResult<Map<byte[], byte[]>> result =
              this.accessor.getExecutor().
                execute(operationContext, read);
            if (result.isEmpty() || null == result.getValue().get(Operation.KV_COL)) {
              // key does not exist -> Not Found
              respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
              helper.finish(NotFound);
              return;
            }
          } catch (Exception e) {
            // something went wrong, internal error
            helper.finish(Error);
            LOG.error("Error during Read: " + e.getMessage(), e);
            respondError(message.getChannel(),
                         HttpResponseStatus.INTERNAL_SERVER_ERROR);
            break;
          }

          // now that we know the key exists, delete it
          try {
            Delete delete = new Delete(table, keyBinary, Operation.KV_COL);
            this.accessor.getExecutor().
              commit(operationContext, delete);
            // deleted successfully
            respondSuccess(message.getChannel(), request);
            helper.finish(Success);
          } catch (Exception e) {
            // something went wrong, internal error
            LOG.error("Error during Delete: " + e.getMessage(), e);
            helper.finish(Error);
            respondError(message.getChannel(),
                         HttpResponseStatus.INTERNAL_SERVER_ERROR);
          }
          break;
        }
        case WRITE: {
          // read the body of the request and add it to the event
          ChannelBuffer content = request.getContent();
          if (content == null) {
            // PUT without content -> 400 Bad Request
            helper.finish(BadRequest);
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
          int length = content.readableBytes();
          byte[] bytes = new byte[length];
          content.readBytes(bytes);
          // create a write and attempt to execute it
          Write write = new Write(table, keyBinary, Operation.KV_COL, bytes);
          try {
            this.accessor.getExecutor().
              commit(operationContext, write);
            // written successfully
            respondSuccess(message.getChannel(), request);
            helper.finish(Success);
          } catch (Exception e) {
            // something went wrong, internal error
            helper.finish(Error);
            LOG.error("Error during Write: " + e.getMessage(), e);
            respondError(message.getChannel(),
                         HttpResponseStatus.INTERNAL_SERVER_ERROR);
          }
          break;
        }
        case CLEAR: {
          // figure out what to clear
          List<ClearFabric.ToClear> toClear = Lists.newArrayList();
          for (String param : clearParams) {
            for (String what : param.split(",")) {
              if ("all".equals(what)) {
                toClear.add(ClearFabric.ToClear.ALL);
              } else if ("data".equals(what)) {
                toClear.add(ClearFabric.ToClear.DATA);
              } else if ("meta".equals(what)) {
                toClear.add(ClearFabric.ToClear.META);
              } else if ("tables".equals(what)) {
                toClear.add(ClearFabric.ToClear.TABLES);
              } else if ("queues".equals(what)) {
                toClear.add(ClearFabric.ToClear.QUEUES);
              } else {
                if ("streams".equals(what)) {
                  toClear.add(ClearFabric.ToClear.STREAMS);
                } else {
                  helper.finish(BadRequest);
                  LOG.trace("Received invalid clear request with URI " +
                              requestUri);
                  respondError(message.getChannel(),
                               HttpResponseStatus.BAD_REQUEST);
                  break;
                }
              }
            }
          }
          ClearFabric clearFabric = new ClearFabric(toClear);
          try {
            this.accessor.getExecutor().
              execute(operationContext, clearFabric);
          } catch (Exception e) {
            LOG.error("Exception clearing data fabric: ", e);
            helper.finish(Error);
            respondError(message.getChannel(),
                         HttpResponseStatus.INTERNAL_SERVER_ERROR);
            break;
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
                  this.accessor.getName() + "'. ", e.getCause());
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
    MetricsHelper.meterError(metrics, this.accessor.getMetricsQualifier());
    LOG.error("Exception caught for connector '" +
                this.accessor.getName() + "'. ", e.getCause());
    if (e.getChannel().isOpen()) {
      respondError(ctx.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }
}
