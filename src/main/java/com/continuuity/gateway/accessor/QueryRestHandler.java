package com.continuuity.gateway.accessor;

import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.gateway.util.NettyRestHandler;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;

import static com.continuuity.common.metrics.MetricsHelper.Status.*;

/**
 * This is the http request handler for the query rest accessor.
 * <p>
 * At this time it only accepts GET requests, which it forwards to a
 * query provider registered in the service discovery.
 * <p>
 * example: GET http://g.c.c/rest-query/feedreader/getfeed?userid=100
 */
public class QueryRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(QueryRestHandler.class);

  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = {
      HttpMethod.GET
  };

  /**
   * All the paths have to be of the form
   * http://host:port&lt;pathPrefix>&lt;service>/&lt;method>?&ltparams>
   * example:
   * <PRE>GET http://g.c.c/rest-query/feedreader/getfeed?userid=100</PRE>
   */
  private String pathPrefix;

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private QueryRestAccessor accessor;

  /**
   * The metrics object of the rest accessor
   */
  private CMetrics metrics;

  /**
   * Constructor requires the accessor that created this
   *
   * @param accessor the accessor that created this
   */
  QueryRestHandler(QueryRestAccessor accessor) {
    this.accessor = accessor;
    this.metrics = accessor.getMetricsClient();
    this.pathPrefix =
        accessor.getHttpConfig().getPathPrefix() +
            accessor.getHttpConfig().getPathMiddle();
  }

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {

    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String uri = request.getUri();

    LOG.trace("Request received: " + method + " " + uri);
    MetricsHelper helper = new MetricsHelper(this.getClass(), this.metrics,
        this.accessor.getMetricsQualifier());

    try {
      // only GET is supported for now
      if (method != HttpMethod.GET) {
        helper.finish(BadRequest);
        LOG.trace("Received a " + method + " request, which is not supported");
        respondNotAllowed(message.getChannel(), allowedMethods);
        return;
      }

      // is this a ping? (http://gw:port/ping) if so respond OK and done
      if ("/ping".equals(uri)) {
        helper.setMethod("ping");
        respondToPing(message.getChannel(), request);
        helper.finish(Success);
        return;
      }

      // parse and verify the url path
      String provider = null, remainder = null;
      // valid paths are <prefix>/service/method?param=value&...
      if (uri.startsWith(this.pathPrefix)) {
        int pos1 = uri.indexOf("/", this.pathPrefix.length());
        int pos2 = uri.indexOf("?", this.pathPrefix.length());
        int pos = (pos1 < 0 ||  pos2 < 0)
            ? Math.max(pos1, pos2) : Math.min(pos1, pos2);
        if (pos > this.pathPrefix.length()) { // at least one char in provider
          provider = uri.substring(this.pathPrefix.length(), pos);
          remainder = uri.substring(pos);
        }
      }
      if (provider == null) {
        helper.finish(BadRequest);
        LOG.trace("Received a request with unsupported path " + uri);
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }
      helper.setMethod(provider);

      // determine the service provider for the given path
      String serviceName = "query." + provider;
      ImmutablePair<String, Integer> pair = this.accessor.
          getServiceDiscovery().getServiceAddress(serviceName);
      if (pair == null) {
        helper.finish(NotFound);
        LOG.trace("Received a request for query provider " + provider + " " +
            "which is not registered. ");
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }

      // make HTTP call to provider with method?param=...
      String relayUri = "http://" + pair.getFirst() + ":" + pair.getSecond()
          + "/v1/query/" + provider + remainder;
      LOG.trace("Relaying request to " + relayUri);

      HttpClient client = new DefaultHttpClient();
      int status;
      byte[] content = null;
      String contentType = null;
      try {
        // TODO use more efficient Http client
        HttpGet get = new HttpGet(relayUri);
        HttpResponse response = client.execute(get);

        // decompose the response
        status = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          if (entity.getContentType() != null)
            contentType = entity.getContentType().getValue();
          int contentLength = (int)entity.getContentLength();
          if (contentLength > 0) {
            InputStream contentStream = entity.getContent();
            byte[] bytes = new byte[contentLength];
            int bytesRead;
            for (bytesRead = 0; bytesRead < contentLength; ) {
              int numBytes = contentStream.read(bytes, bytesRead,
                  contentLength - bytesRead);
              if (numBytes < 0) break;
              bytesRead += numBytes;
            }
            contentStream.close();
            if (bytesRead == contentLength)
              content = bytes;
            else
              content = Arrays.copyOf(bytes, bytesRead);
          }
        }
      } catch (Exception e) {
        LOG.error("Exception when forwarding query to URI " + relayUri
            + ": " + e.getMessage() + ", at " +
            StackTraceUtil.toStringStackTrace(e));
        helper.finish(Error);
        respondError(message.getChannel(),
            HttpResponseStatus.INTERNAL_SERVER_ERROR);
        return;
      } finally {
        client.getConnectionManager().shutdown();
      }

      // return result from provider
      respond(message.getChannel(), request,
          HttpResponseStatus.valueOf(status),
          contentType == null ? null : Collections.
              singletonMap(HttpHeaders.Names.CONTENT_TYPE, contentType),
          content);

      if (status == HttpStatus.SC_OK) {
        helper.finish(Success);
      } else if (status == HttpStatus.SC_NOT_FOUND) {
        helper.finish(NotFound);
      } else if (status == HttpStatus.SC_NO_CONTENT) {
        helper.finish(NoData);
      } else if (status == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
        helper.finish(Error);
      } else if (status == HttpStatus.SC_BAD_REQUEST) {
        helper.finish(BadRequest);
      } else { // not sure what this is, we will meter as a bad request
        helper.finish(BadRequest);
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
    if(e.getChannel().isOpen()) {
      respondError(e.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }
}
