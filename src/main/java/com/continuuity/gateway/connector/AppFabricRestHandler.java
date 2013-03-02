package com.continuuity.gateway.connector;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.discovery.Discoverable;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.NettyRestHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the rest accessor. At this time it
 * only accepts GET requests to retrieve a value for a key from a named table.
 */
public class AppFabricRestHandler extends NettyRestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricRestHandler.class);
  private final static String ARCHIVE_NAME_HEADER = "X-Archive-Name";
  private final static String APPFABRIC_SERVICE_NAME = "app.fabric.service";

  /**
   * The allowed methods for this handler
   */
  Set<HttpMethod> allowedMethods = Collections.singleton(
      HttpMethod.PUT);

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private AppFabricRestConnector connector;

  /**
   * The metrics object of the rest connector
   */
  private final CMetrics metrics;

  private final String pathPrefix;

  /**
   * Constructor requires the connector that created this
   *
   * @param connector the connector that created this
   */
  AppFabricRestHandler(AppFabricRestConnector connector) {
    this.connector = connector;
    this.metrics = connector.getMetricsClient();
    this.pathPrefix =
        connector.getHttpConfig().getPathPrefix() +
        connector.getHttpConfig().getPathMiddle();
  }

  @Override
  public void messageReceived(ChannelHandlerContext context,
                              MessageEvent message) throws Exception {
    HttpRequest request = (HttpRequest) message.getMessage();
    MetricsHelper helper = new MetricsHelper(this.getClass(), this.metrics, this.connector.getMetricsQualifier());
    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();

    // Ping doesn't need a auth token.
    if ("/ping".equals(requestUri)) {
      helper.setMethod("ping");
      respondToPing(message.getChannel(), request);
      helper.finish(Success);
      return;
    }

    if(! connector.getAuthenticator().authenticateRequest(request)) {
      respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
      helper.finish(BadRequest);
      return;
    }

    String accountId = connector.getAuthenticator().getAccountId(request);
    if(accountId == null || accountId.isEmpty()) {
      respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
      helper.finish(BadRequest);
      return;
    }

    try {
      if(method != HttpMethod.PUT) {
        LOG.trace("Received Unsupported http request method " + method.getName());
        respondNotAllowed(message.getChannel(), allowedMethods);
        helper.finish(BadRequest);
        return;
      }

      AppFabricService.Client client = getAppFabricClient();
      Preconditions.checkArgument(client != null);

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));

      try {
        if(requestUri.contains("/apps/status")) {
          ResourceIdentifier rIdentifier = new ResourceIdentifier(accountId, "no-app", "no-res", 1);
          DeploymentStatus status = client.dstatus(token, rIdentifier);
          Map<String, String> headers = Maps.newHashMap();
          headers.put(HttpHeaders.Names.CONTENT_TYPE, "application/json");
          respond(message.getChannel(), request, HttpResponseStatus.OK, headers, getJsonStatus(status.getOverall(),
                                                                                               status.getMessage())
            .toString().getBytes(Charset.forName("UTF-8")));
          helper.finish(Success);
        } else {
          String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);
          if(archiveName == null || archiveName.isEmpty()) {
            LOG.trace("Archive name was not available in the header");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            helper.finish(BadRequest);
            return;
          }

          ChannelBuffer content = request.getContent();
          if(content == null) {
            LOG.trace("No body passed from client");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            helper.finish(BadRequest);
            return;
          }

          ResourceInfo rInfo = new ResourceInfo(accountId, "GWApp", archiveName,1, System.currentTimeMillis()/1000);
          ResourceIdentifier rIdentifier = client.init(token, rInfo);

          while(content.readableBytes() > 0) {
            int bytesToRead = Math.min(1024 * 100, content.readableBytes());
            client.chunk(token, rIdentifier, content.readSlice(bytesToRead).toByteBuffer());
          }

          client.deploy(token, rIdentifier);
          respondSuccess(message.getChannel(), request);
        }
      } finally {
        if(client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if(client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
    } catch (Exception e) {
      LOG.debug(StackTraceUtil.toStringStackTrace(e));
      LOG.error("Exception caught for connector '" + this.connector.getName() + "'. ", e.getCause());
      helper.finish(Error);
      if (message.getChannel().isOpen()) {
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        message.getChannel().close();
      }
    }
  }

  private JsonObject getJsonStatus(int status, String message) {
    JsonObject object = new JsonObject();
    object.addProperty("status", status);
    object.addProperty("message", message);
    return object;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx,
                              ExceptionEvent e)
      throws Exception {
    MetricsHelper.meterError(metrics, this.connector.getMetricsQualifier());
    LOG.error("Exception caught for connector '" +
        this.connector.getName() + "'. ", e.getCause());
    if (e.getChannel().isOpen()) {
      respondError(ctx.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }

  private AppFabricService.Client getAppFabricClient() throws TTransportException  {
    List<Discoverable> endpoints
      = Lists.newArrayList(connector.getDiscoveryServiceClient().discover(APPFABRIC_SERVICE_NAME));
    if(endpoints.isEmpty()) {
      LOG.trace("Received a request for deploy, but AppFabric service was not available.");
      return null;
    }
    Collections.shuffle(endpoints);

    InetSocketAddress endpoint = endpoints.get(0).getSocketAddress();
    TTransport transport = new TFramedTransport(new TSocket(endpoint.getHostName(), endpoint.getPort()));
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    return new AppFabricService.Client(protocol);
  }
}
