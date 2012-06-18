package com.continuuity.gateway.accessor;

import com.continuuity.api.data.Delete;
import com.continuuity.api.data.Read;
import com.continuuity.api.data.ReadKeys;
import com.continuuity.api.data.Write;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.gateway.util.Util;
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
 * This is the http request handler for the rest accessor. At this time it only accepts
 * GET requests to retrieve a value for a key from a named table.
 */
public class RestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(RestHandler.class);

  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = {HttpMethod.GET, HttpMethod.DELETE, HttpMethod.PUT};

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

  /**
   * Disallow default constructor
   */
  @SuppressWarnings("unused")
  private RestHandler() {  }

  /**
   * Constructor requires the accessor that created this
   *
   * @param accessor the accessor that created this
   */
  RestHandler(RestAccessor accessor) {
    this.accessor = accessor;
    this.pathPrefix = accessor.getHttpConfig().getPathPrefix() + accessor.getHttpConfig().getPathMiddle();
  }

  private static final int UNKNOWN = 0;
  private static final int READ = 1;
  private static final int WRITE = 2;
  private static final int DELETE = 3;
  private static final int LIST = 4;

  @Override
  public void messageReceived(ChannelHandlerContext context, MessageEvent message) throws Exception {
    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();

    LOG.debug("Request received: " + method + " " + requestUri);

    // we only support get requests for now
    if (method != HttpMethod.GET && method != HttpMethod.DELETE && method != HttpMethod.PUT) {
      LOG.debug("Received a " + method + " request, which is not supported");
      respondNotAllowed(message.getChannel(), allowedMethods);
      return;
    }

    // based on the request URL, determine what to do
    QueryStringDecoder decoder = new QueryStringDecoder(requestUri);
    Map<String, List<String>> parameters = decoder.getParameters();
    int operation = UNKNOWN;
    if (method == HttpMethod.PUT)
      operation = WRITE;
    else if (method == HttpMethod.DELETE)
      operation = DELETE;
    else if (method == HttpMethod.GET) {
      if (parameters == null || parameters.size() == 0)
        operation = READ;
      else {
        List<String> qParams = parameters.get("q");
        if (qParams != null && qParams.size() == 1 && "list".equals(qParams.get(0)))
          operation = LIST;
    } }

    // respond with error for unknown requests
    if (operation == UNKNOWN) {
      LOG.debug("Received an unsupported " + method + " request '" + request.getUri() + "'.");
      respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
      return;
    }

    // respond with error for parameters if the operation does not allow them
    if (operation != LIST && parameters != null && !parameters.isEmpty()) {
      LOG.debug("Received a " + method + " request with query parameters, which is not supported");
      respondError(message.getChannel(), HttpResponseStatus.NOT_IMPLEMENTED);
      return;
    }

    // we only support requests of the form POST http://host:port/prefix/path/<tablename>/<key>
    String destination = null, key = null;
    String path = decoder.getPath();
    if (path.startsWith(this.pathPrefix)) {
      String remainder = path.substring(this.pathPrefix.length());
      int pos = remainder.indexOf("/");
      if (pos < 0) {
        destination = remainder;
        key = null;
      } else {
        destination = remainder.substring(0, pos);
        // no further / is allowed in the path
        if (remainder.length() == pos + 1) {
          key = null;
        } else if (remainder.indexOf('/', pos + 1) < 0)
          key = remainder.substring(pos + 1);
        else {
          LOG.debug("Received a request with invalid path " + path + "(path does not end with key)");
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
          return;
    } } }

    // check that URL could be parsed up to destination
    if (destination == null) {
      LOG.debug("Received a request with unknown path '" + path + "'.");
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      return;
    }

    // all operations except for LIST need a key
    if (operation != LIST && (key == null || key.length() == 0)) {
      LOG.debug("Received a request with invalid path " + path + "(no key given)");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      return;
    }
    // operation LIST must not have a key
    if (operation == LIST && (key != null && key.length() > 0)) {
      LOG.debug("Received a request with invalid path " + path + "(no key may be given)");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      return;
    }

    // check that destination is valid - for now only "default" is allowed
    if (!"default".equals(destination)) {
      LOG.debug("Received a request with path " + path + " for destination other than 'default'");
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      return;
    }

    // key is URL-encoded, decode it
    byte[] keyBinary = null;
    if (key != null) {
      key = URLDecoder.decode(key, "ISO8859_1");
      LOG.debug("Received " + method + " request for key '" + key + "'.");
      keyBinary = key.getBytes("ISO8859_1");
    }

    switch(operation) {
      case READ : {
        // Get the value from the data fabric
        byte[] value;
        try {
          Read read = new Read(keyBinary);
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
        break;
      }
      case LIST : {
        int start = 0, limit = 100;
        String encoding = "url";
        List<String> startParams = parameters.get("start");
        if (startParams != null && !startParams.isEmpty()) {
          try {
            start = Integer.valueOf(startParams.get(0));
          } catch (NumberFormatException e) {
            LOG.debug("Received a request with invalid start '" + startParams.get(0) + "' (not an integer).");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
        }
        List<String> limitParams = parameters.get("limit");
        if (limitParams != null && !limitParams.isEmpty()) {
          try {
            limit = Integer.valueOf(limitParams.get(0));
          } catch (NumberFormatException e) {
            LOG.debug("Received a request with invalid limit '" + limitParams.get(0) + "' (not an integer).");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
        }
        List<String> encParams = parameters.get("enc");
        if (encParams != null && !encParams.isEmpty()) {
          encoding = encParams.get(0);
          if (!"hex".equals(encoding) && !"url".equals(encoding) && !Charset.isSupported(encoding)) {
            LOG.debug("Received a request with invalid encoding '" + encoding + "'.");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            return;
          }
        }
        List<byte[]> keys = null;
        try {
          ReadKeys read = new ReadKeys(start, limit);
          keys = this.accessor.getExecutor().execute(read);
        } catch (Exception e) {
          LOG.error("Error listing keys: " + e.getMessage() + ".", e);
          respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
          return;
        }
        if (keys == null) {
          // something went wrong, internal error
          respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
          return;
        }
        StringBuilder builder = new StringBuilder();
        for (byte[] keyBytes : keys) {
          builder.append(Util.encode(keyBytes, encoding));
          builder.append('\n');
        }
        // if encoding was hex or url, send it back as ASCII, otherwise use encoding
        byte[] responseBody = builder.toString().getBytes(
            "url".equals(encoding) || "hex".equals(encoding) ? "ASCII" : encoding);
        respondSuccess(message.getChannel(), request, responseBody);
        break;
      }
      case DELETE : {
        // first perform a Read to determine whether the key exists
        Read read = new Read(keyBinary);
        byte[] value = this.accessor.getExecutor().execute(read);
        if (value == null) {
          // key does not exist -> Not Found
          respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
          return;
        }
        Delete delete = new Delete(keyBinary);
        if (this.accessor.getExecutor().execute(delete)) {
          // deleted successfully
          respondSuccess(message.getChannel(), request, null);
        } else {
          // something went wrong, internal error
          respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
        break;
      }
      case WRITE : {
        // read the body of the request and add it to the event
        ChannelBuffer content = request.getContent();
        if (content == null) {
          // PUT without content -> 400 Bad Request
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
          return;
        }
        int length = content.readableBytes();
        byte[] bytes = new byte[length];
        content.readBytes(bytes);
        // create a write and attempt to execute it
        Write write = new Write(keyBinary, bytes);
        if (this.accessor.getExecutor().execute(write)) {
          // written successfully
          respondSuccess(message.getChannel(), request, null);
        } else {
          // something went wrong, internal error
          respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
        break;
      }
      default: {
        // this should not happen because we already checked above -> internal error
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
    LOG.error("Exception caught for connector '" + this.accessor.getName() + "'. ", e.getCause());
    e.getChannel().close();
  }
}
