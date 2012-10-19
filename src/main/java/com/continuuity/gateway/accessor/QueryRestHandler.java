package com.continuuity.gateway.accessor;

import com.continuuity.common.metrics.CMetrics;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.util.NettyRestHandler;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;

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

    LOG.debug("Request received: " + method + " " + uri);
    metrics.meter(this.getClass(), Constants.METRIC_REQUESTS, 1);

    // only GET is supported for now
    if (method != HttpMethod.GET) {
      LOG.debug("Received a " + method + " request, which is not supported");
      respondNotAllowed(message.getChannel(), allowedMethods);
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
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
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      LOG.debug("Received a request with unsupported path " + uri);
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      return;
    }

    // determine the service provider for the given path
    String serviceAddress = this.accessor.getProviderDiscovery()
          .getServiceAddress(provider);
    if (serviceAddress == null) {
      metrics.meter(this.getClass(), Constants.METRIC_BAD_REQUESTS, 1);
      LOG.debug("Received a request for query provider " + provider + " " +
          "which is not registered. ");
      respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
      return;
    }

    // make HTTP call to provider with method?param=...
    String relayUri = "http://" + serviceAddress + "/v1/query/" +
        provider + remainder;
    LOG.debug("Relaying request to " + relayUri);

    // TODO use more efficient Http client
    HttpGet get = new HttpGet(relayUri);
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(get);

    // decompose the response
    int status = response.getStatusLine().getStatusCode();
    byte[] content = null;
    String contentType = null;
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
    client.getConnectionManager().shutdown();

    // return result from provider
    respond(message.getChannel(), request,
        HttpResponseStatus.valueOf(status),
        contentType == null ? null : Collections.
            singletonMap(HttpHeaders.Names.CONTENT_TYPE, contentType),
        content);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      throws Exception {
    metrics.meter(this.getClass(), Constants.METRIC_INTERNAL_ERRORS, 1);
    LOG.error("Exception caught for connector '" +
        this.accessor.getName() + "'. ", e.getCause());
    if(e.getChannel().isOpen()) {
      respondError(e.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }
}
