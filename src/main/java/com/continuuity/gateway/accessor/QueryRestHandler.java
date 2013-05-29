package com.continuuity.gateway.accessor;

import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.gateway.GatewayMetricsHelperWrapper;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
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
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NoData;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the query rest accessor.
 * <p/>
 * At this time it only accepts POST requests, which it forwards to a
 * procedure registered in the service discovery.
 * <p/>
 * example:
 * <pre>
 * POST http://g.c.c/query/myapp/feedreader/getfeed HTTP/1.1
 * Host: g.c.c
 *
 * {"userid":100}
 * </pre>
 */
public final class QueryRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
    .getLogger(QueryRestHandler.class);

  /**
   * The allowed methods for this handler
   */
  private static final Set<HttpMethod> allowedMethods = Collections.singleton(
    HttpMethod.POST);

  /**
   * All the paths have to be of the form
   * http://host:port&lt;pathPrefix>&lt;application>/&lt;service>/&lt;method>
   * example:
   * <pre>
   * POST http://g.c.c/query/myapp/feedreader/getfeed HTTP/1.1
   * Host: g.c.c
   *
   * {"userid":100}
   * </pre>
   */
//  private String pathPrefix;

  private final Pattern pathPattern;

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private final QueryRestAccessor accessor;

  /**
   * The metrics object of the rest accessor
   */
  private final CMetrics metrics;

  /**
   * Constructor requires the accessor that created this
   *
   * @param accessor the accessor that created this
   */
  QueryRestHandler(QueryRestAccessor accessor) {
    this.accessor = accessor;
    this.metrics = accessor.getMetricsClient();
    this.pathPattern = Pattern.compile(String.format("%s%s(.+)/(.+)/(.+)",
                                                     accessor.getHttpConfig().getPathPrefix(),
                                                     accessor.getHttpConfig().getPathMiddle()));
  }

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {

    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String uri = request.getUri();

    LOG.trace("Request received: " + method + " " + uri);
    GatewayMetricsHelperWrapper helper = new GatewayMetricsHelperWrapper(new MetricsHelper(
      this.getClass(), this.metrics, this.accessor.getMetricsQualifier()), accessor.getGatewayMetrics());

    try {
      // only POST is supported for now
      if (!method.equals(HttpMethod.POST)) {
        helper.finish(BadRequest);
        LOG.trace("Received a " + method + " request, which is not supported");
        respondNotAllowed(message.getChannel(), allowedMethods);
        return;
      }

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

      // is this a ping? (http://gw:port/ping) if so respond OK and done
      if ("/ping".equals(uri)) {
        helper.setMethod("ping");
        respondToPing(message.getChannel(), request);
        helper.finish(Success);
        return;
      }

      Matcher pathMatcher = pathPattern.matcher(URI.create(uri).getPath());
      if (!pathMatcher.matches()) {
        helper.finish(BadRequest);
        LOG.trace("Received a request with unsupported path " + uri);
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }
      String applicationId = pathMatcher.group(1);
      String procedureName = pathMatcher.group(2);
      String methodName = pathMatcher.group(3);

      helper.setMethod(procedureName);

      // determine the service provider for the given path
      String serviceName = String.format("procedure.%s.%s.%s", accountId, applicationId, procedureName);
      List<Discoverable> endpoints = Lists.newArrayList(accessor.getDiscoveryServiceClient().discover(serviceName));

      if (endpoints.isEmpty()) {
        helper.finish(NotFound);
        LOG.trace("Received a request for procedure " + procedureName +
                    " in application " + applicationId + " which is not registered. ");
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }

      // make HTTP call to provider
      Collections.shuffle(endpoints);
      InetSocketAddress endpoint = endpoints.get(0).getSocketAddress();
      String relayUri = Joiner.on('/').appendTo(
        new StringBuilder("http://").append(endpoint.getHostName()).append(":").append(endpoint.getPort()).append("/"),
        "apps", applicationId, "procedures", procedureName, methodName).toString();

      LOG.trace("Relaying request to " + relayUri);

      HttpClient client = new DefaultHttpClient();
      int status;
      ChannelBuffer content = ChannelBuffers.EMPTY_BUFFER;
      String contentType = null;
      try {
        // TODO use more efficient Http client
        ChannelBuffer requestContent = request.getContent();
        HttpPost post = new HttpPost(relayUri);
        post.setEntity(
          new InputStreamEntity(new ChannelBufferInputStream(requestContent), requestContent.readableBytes()));

        HttpResponse response = client.execute(post);

        // decompose the response
        status = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          if (entity.getContentType() != null) {
            contentType = entity.getContentType().getValue();
          }
          int contentLength = (int) entity.getContentLength();
          if (contentLength > 0) {
            content = ChannelBuffers.dynamicBuffer(contentLength);
          } else {
            // the transfer encoding is usually chunked, so no content length is provided. Just trying to read anything
            content = ChannelBuffers.dynamicBuffer();
          }
          InputStream input = entity.getContent();
          try {
            ByteStreams.copy(input, new ChannelBufferOutputStream(content));
          } finally {
            input.close();
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
    if (e.getChannel().isOpen()) {
      respondError(e.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }
}
