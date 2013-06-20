package com.continuuity.gateway.connector;

import com.continuuity.api.common.Bytes;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.FlowDescriptor;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.FlowStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.gateway.GatewayMetricsHelperWrapper;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.internal.app.services.legacy.FlowDefinitionImpl;
import com.continuuity.internal.app.services.legacy.FlowletDefinition;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the Http request handler for the AppFabric rest connector. At this time it accepts GET, POST and PUT
 * requests. REST calls can be used to deploy an application, start or stop, or get the status of a flow, procedure
 * or a map reduce job. It also supports reading and changing the number of instances of a flowlet.
 * a Post of http://<hostname>:<port>/app with the jar file of the to be deployed app in the content of the request
 * can be used to deploy a new app.
 * a Get http://<hostname>:<port>/app/status returns the current deployment status
 * a Post /app/<app-id>/<entity-type>/<flow id>?op=<op> with <op> as start, stop to start a flow, procedure or mapreduce
 * a Get  /app/<app-id>/<entity-type>/<flow id>?op=status to get the status of a flow, procedure or mapreduce
 * a Put/Get /app/<app-id>/flow/<flow id>/<flowlet id>?op=instances to change or get the number of flowlet instances
 */
public class AppFabricRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricRestHandler.class);

  private static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";
  private static final String APPFABRIC_SERVICE_NAME = "app.fabric.service";

  /**
   * The allowed methods for this handler.
   */
  private static final Set<HttpMethod> ALLOWED_HTTP_METHODS = Sets.newHashSet(HttpMethod.PUT, HttpMethod.POST,
                                                                              HttpMethod.GET);

  private static final String DEPLOY_PATH = "";

  private static final String DEPLOY_STATUS_PATH = "/status";

  private static final String FLOW_START_STOP_PATH =
    "/([A-Za-z0-9_]+)/(flow|procedure|mapreduce)/([A-Za-z0-9_]+)\\?op=(start|stop)";

  private static final String FLOW_STATUS_PATH =
    "/([A-Za-z0-9_]+)/(flow|procedure|mapreduce)/([A-Za-z0-9_]+)\\?op=status";

  private static final String FLOWLET_INSTANCES_PATH =
    "/([A-Za-z0-9_]+)/flow/([A-Za-z0-9_]+)/([A-Za-z0-9_]+)\\?op=instances";

  /**
   * The allowed URI and Http methods for this handler.
   */
//  private static final Map<String, List<HttpMethod>> ALLOWED_PATHS = generateAllowedURIs();
  private static final Map<String, ImmutablePair<List<HttpMethod>, Pattern>> ALLOWED_PATHS =
    ImmutableMap.of(
      DEPLOY_PATH,
        new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.POST),
                                                     Pattern.compile(DEPLOY_PATH)),
      DEPLOY_STATUS_PATH,
        new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET),
                                                     Pattern.compile(DEPLOY_STATUS_PATH)),
      FLOW_START_STOP_PATH,
        new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.POST),
                                                     Pattern.compile(FLOW_START_STOP_PATH)),
      FLOW_STATUS_PATH,
        new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET),
                                                     Pattern.compile(FLOW_STATUS_PATH)),
      FLOWLET_INSTANCES_PATH,
      new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET, HttpMethod.PUT),
                                                   Pattern.compile(FLOWLET_INSTANCES_PATH))
    );
  /**
   * Will help validate URL paths, authenticate and get a metrics helper
   */
  private AppFabricRestConnector connector;

  /**
   * The metrics object of this rest connector.
   */
  private final CMetrics metrics;

  // This is the prefix that all valid URLs must have - except '/ping'.
  private final String pathPrefix;

  // The metrics helper.
  GatewayMetricsHelperWrapper metricsHelper;

  /**
   * Constructor requires the connector that created this.
   *
   * @param connector the connector that created this
   */
  AppFabricRestHandler(AppFabricRestConnector connector) {
    this.connector = connector;
    metrics = connector.getMetricsClient();
    pathPrefix = connector.getHttpConfig().getPathPrefix() + connector.getHttpConfig().getPathMiddle();
    metricsHelper = new GatewayMetricsHelperWrapper(new MetricsHelper(this.getClass(), metrics,
                                                                      connector.getMetricsQualifier()),
                                                    connector.getGatewayMetrics());
  }

  String matchURI(String requestUri, HttpMethod method, MessageEvent message) {
    if (requestUri.startsWith(pathPrefix)) {
      String path = requestUri.substring(pathPrefix.length());
      for (Map.Entry<String, ImmutablePair<List<HttpMethod>, Pattern>> uriPattern : ALLOWED_PATHS.entrySet()) {
        if (path.matches(uriPattern.getKey())) {
          if (uriPattern.getValue().getFirst().contains(method)) {
            return uriPattern.getKey();
          } else {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Received a {} request, which is not supported by '{}'.", method, requestUri);
            }
            respondNotAllowed(message.getChannel(), uriPattern.getValue().getFirst());
            metricsHelper.finish(BadRequest);
            return null;
          }
        }
      }
    }
    respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST,
                 String.format("Received a request with an unsupported path '%s'.", requestUri), true);
    metricsHelper.finish(BadRequest);
    return null;
  }

  @Override
  public void messageReceived(ChannelHandlerContext context, MessageEvent message) throws Exception {

    // first decode the request
    HttpRequest request = (HttpRequest) message.getMessage();
    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();
    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    String path = decoder.getPath();

    // log the request
    if (LOG.isTraceEnabled()) {
      LOG.trace("Request received: {} {}.", method, requestUri);
    }

    try {
      // check whether the request's HTTP method is supported
      if (!ALLOWED_HTTP_METHODS.contains(method)) {
        LOG.trace("Received a {} request, which is not supported (path is + '{}').", method, path);
        respondNotAllowed(message.getChannel(), ALLOWED_HTTP_METHODS);
        metricsHelper.finish(BadRequest);
        return;
      }

      // ping doesn't need an auth token.
      if ("/ping".equals(requestUri)) {
        if (!HttpMethod.GET.equals(method)) {
          LOG.trace("Received a {} request, which is not supported (path is + '{}')", method, path);
          respondNotAllowed(message.getChannel(), ImmutableList.of(HttpMethod.GET));
          metricsHelper.finish(BadRequest);
        } else {
          metricsHelper.setMethod("ping");
          respondToPing(message.getChannel(), request);
          metricsHelper.finish(Success);
        }
        return;
      }

      String opPath = matchURI(requestUri, method, message);

      if (opPath == null) {
        return;
      }

      // if authentication is enabled, verify an authentication token has been
      // passed and then verify it is valid
      if (!connector.getAuthenticator().authenticateRequest(request)) {
        respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
        metricsHelper.finish(BadRequest);
        return;
      }
      String accountId = connector.getAuthenticator().getAccountId(request);
      if (accountId == null || accountId.isEmpty()) {
        LOG.info("No valid account information found.");
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        metricsHelper.finish(BadRequest);
        return;
      }

      AppFabricService.Client client = getAppFabricClient();
      Preconditions.checkArgument(client != null);

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));

      try {
        if (opPath.equals(DEPLOY_PATH)) { // Plain '/app' to deploy an app
          String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);
          if (archiveName == null || archiveName.isEmpty()) {
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST,
                         "Archive name was not available in the Http header.", true);
            metricsHelper.finish(BadRequest);
            return;
          }

          ChannelBuffer content = request.getContent();
          if (content == null) {
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST, "No http body passed from client.", true);
            metricsHelper.finish(BadRequest);
            return;
          }

          ResourceInfo rInfo = new ResourceInfo(accountId, "GWApp", archiveName, 1, System.currentTimeMillis() / 1000);
          ResourceIdentifier rIdentifier = client.init(token, rInfo);

          while (content.readableBytes() > 0) {
            int bytesToRead = Math.min(1024 * 100, content.readableBytes());
            client.chunk(token, rIdentifier, content.readSlice(bytesToRead).toByteBuffer());
          }

          client.deploy(token, rIdentifier);
          respondSuccess(message.getChannel(), request);
          metricsHelper.finish(Success);
          return;
        }

        if (opPath.equals(DEPLOY_STATUS_PATH)) { // '/app/status'
          ResourceIdentifier rIdentifier = new ResourceIdentifier(accountId, "no-app", "no-res", 1);
          DeploymentStatus status = client.dstatus(token, rIdentifier);
          byte[] response =
            getJsonStatus(status.getOverall(), status.getMessage()).toString().getBytes(Charset.forName("UTF-8"));
          respondJson(message.getChannel(), request, HttpResponseStatus.OK, response);
          metricsHelper.finish(Success);
          return;
        }

        // from here on, path is either
        // /app/<app-id>/<entity-type>/<flow id>?op=<op> with <op> as start, stop, status
        // or
        // /app/<app-id>/flow/<flow id>/<flowlet id>?op=instances

        if (opPath.equals(FLOW_START_STOP_PATH)) {
          Pattern p = ALLOWED_PATHS.get(FLOW_START_STOP_PATH).getSecond();
          Matcher m = p.matcher(requestUri.substring(pathPrefix.length()));
          m.find();
          FlowIdentifier flowIdent = new FlowIdentifier(accountId, m.group(1), m.group(3), 1);
          flowIdent.setType(getEntityType(m.group(2)));
          handleFlowOperation(message, request, metricsHelper, client, token, flowIdent, m.group(4));
        }

        if (opPath.equals(FLOW_STATUS_PATH)) {
          Pattern p = ALLOWED_PATHS.get(FLOW_STATUS_PATH).getSecond();
          Matcher m = p.matcher(requestUri.substring(pathPrefix.length()));
          m.find();
          FlowIdentifier flowIdent = new FlowIdentifier(accountId, m.group(1), m.group(3), 1);
          flowIdent.setType(getEntityType(m.group(2)));
          handleFlowOperation(message, request, metricsHelper, client, token, flowIdent, "status");
        }

        if (opPath.equals(FLOWLET_INSTANCES_PATH)) {
          Pattern p = ALLOWED_PATHS.get(FLOWLET_INSTANCES_PATH).getSecond();
          Matcher m = p.matcher(requestUri.substring(pathPrefix.length()));
          m.find();
          FlowIdentifier flowIdent = new FlowIdentifier(accountId, m.group(1), m.group(2), 1);
          flowIdent.setType(EntityType.FLOW);
          handleFlowletOperation(message, request, metricsHelper, client, token, flowIdent, m.group(3));
        }

      } finally {
         if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
    } catch (Exception e) {
      LOG.debug(StackTraceUtil.toStringStackTrace(e));
      LOG.error("Exception caught for connector '{}'. ", this.connector.getName(), e.getCause());
      metricsHelper.finish(Error);
      if (message.getChannel().isOpen()) {
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        message.getChannel().close();
      }
    }
  }

  private void handleFlowOperation(MessageEvent message, HttpRequest request, GatewayMetricsHelperWrapper metricsHelper,
                                   AppFabricService.Client client, AuthToken token, FlowIdentifier flowIdent,
                                   String operation)
    throws TException, AppFabricServiceException {

    // ignoring that flow might be running already when starting flow; or has been stopped before trying to stop flow
    if ("start".equals(operation)) {
      try {
        // looking for optional Map<String, String> in Json format in content of request body
        InputStreamReader reader =
          new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
        Map<String, String> arguments = new Gson().fromJson(reader, new TypeToken<Map<String, String>>() {}.getType());
        if (arguments == null) {
          arguments = ImmutableMap.of();
        }
        client.start(token, new FlowDescriptor(flowIdent, arguments));
        // returning success, application needs to retrieve status of flow to verify that flow is actually running
        respondSuccess(message.getChannel(), request);
        metricsHelper.finish(Success);
      }  catch (JsonParseException e) {
        // failed to parse json, so respond with bad request
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST,
                     String.format("Failed to read arguments for flow as JSON from request: %s.", e.getMessage()), true);
        metricsHelper.finish(BadRequest);
        return;
      }

    } else if ("stop".equals(operation)) {
      client.stop(token, flowIdent);
      // returning success, application needs to retrieve status of flow to verify that flow has been stopped
      respondSuccess(message.getChannel(), request);
      metricsHelper.finish(Success);

    } else if ("status".equals(operation)) {
      FlowStatus flowStatus = client.status(token, flowIdent);
      if (flowStatus != null) {
        byte[] response = Bytes.toBytes("{\"status\":" + flowStatus.getStatus() + "}");
        respondJson(message.getChannel(), request, HttpResponseStatus.OK, response);
        metricsHelper.finish(Success);
      } else {
        LOG.trace("Flow status could not be retrieved.");
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        metricsHelper.finish(Error);
      }
    }
  }

  private void handleFlowletOperation(MessageEvent message, HttpRequest request,
                                      GatewayMetricsHelperWrapper metricsHelper, AppFabricService.Client client,
                                      AuthToken token, FlowIdentifier flowIdent, String flowletId)
    throws TException, AppFabricServiceException {

    if (request.getMethod() == HttpMethod.GET) { // retrieve number of flowlet instances
      String flowDefJson = client.getFlowDefinition(flowIdent);
      if (flowDefJson == null) {
        LOG.error("Failed to get number of flowlet instances for flowlet {}.", flowletId);
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        metricsHelper.finish(Error);
        return;
      }
      FlowDefinitionImpl flowDef = new Gson().fromJson(flowDefJson, FlowDefinitionImpl.class);
      for (FlowletDefinition flowletDef : flowDef.getFlowlets()) {
        if (flowletDef.getName().equals(flowletId)) {
          byte[] response = Bytes.toBytes("{\"instances\":" + flowletDef.getInstances() + "}");
          respondJson(message.getChannel(), request, HttpResponseStatus.OK, response);
          metricsHelper.finish(Success);
          return;
        }
      }
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST,
                   String.format("Flowlet %s does not exist.", flowletId), true);
      metricsHelper.finish(BadRequest);

    } else if (request.getMethod() == HttpMethod.PUT) {  // set number of flowlet instances
      // looking for Json string {"instances":<number>} in content of request body
      try {
        InputStreamReader reader =
          new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
        Map<String, String> valueMap = new Gson().fromJson(reader, new TypeToken<Map<String, String>>() {}.getType());
        if (valueMap.get("instances") == null) {
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST,
                       "Parameter 'instances' is missing in JSON body of Http Request.", true);
          metricsHelper.finish(BadRequest);
        } else {
          short numInstances = Short.parseShort(valueMap.get("instances"));
          if (numInstances < 1) {
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST,
                         "Number of specified flowlet instances has to be greather than 0.", true);
            metricsHelper.finish(BadRequest);
          } else {
            client.setInstances(token, flowIdent, flowletId, numInstances);
            LOG.trace("Changed number of flowlet instances to {}.", numInstances);
            respondSuccess(message.getChannel(), request);
            metricsHelper.finish(Success);
          }
        }
        return;
      } catch (NumberFormatException e) {
        // failed to parse number of instances as short value
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST,
                     String.format("Failed to parse number of flowlet instances: %s.", e.getMessage()), true);
        metricsHelper.finish(BadRequest);
        return;
      } catch (JsonParseException e) {
        // failed to parse json, so respond with bad request
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST,
                     String.format("Failed to read body as JSON: %s.", e.getMessage()), true);
        metricsHelper.finish(BadRequest);
        return;
      } catch (Exception e) {
        // failed to get number of flowlet instances, so respond with bad request
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST,
                     String.format("Failed to get number of flowlet instances from request: %s.", e.getMessage()),
                     true);
        metricsHelper.finish(BadRequest);
        return;
      }

    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    MetricsHelper.meterError(metrics, this.connector.getMetricsQualifier());
    LOG.error("Exception caught for connector '" +
                this.connector.getName() + "'. ", e.getCause());
    if (e.getChannel().isOpen()) {
      respondError(ctx.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }

  private void respondError(Channel channel, HttpResponseStatus status, String reason, boolean logTrace) {
    if (logTrace && LOG.isTraceEnabled()) {
      LOG.trace(reason);
    }
    HttpResponse response = new DefaultHttpResponse(
      HttpVersion.HTTP_1_1, status);
    if (reason != null) {
      ChannelBuffer body = ChannelBuffers.wrappedBuffer(Charsets.UTF_8.encode(reason));
      response.addHeader(HttpHeaders.Names.CONTENT_LENGTH, body.readableBytes());
      response.setContent(body);
    } else {
      response.addHeader(HttpHeaders.Names.CONTENT_LENGTH, 0);
    }
    ChannelFuture future = channel.write(response);
    future.addListener(ChannelFutureListener.CLOSE);
  }

  private AppFabricService.Client getAppFabricClient() throws TTransportException {
    List<Discoverable> endpoints
      = Lists.newArrayList(connector.getDiscoveryServiceClient().discover(APPFABRIC_SERVICE_NAME));
    if (endpoints.isEmpty()) {
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

  private static EntityType getEntityType(String entityType) {
    if ("flow".equals(entityType)) {
      return EntityType.FLOW;
    } else if ("procedure".equals(entityType)) {
      return EntityType.QUERY;
    } else if ("mapreduce".equals(entityType)) {
      return EntityType.MAPREDUCE;
    } else {
      return null;
    }
  }
}
