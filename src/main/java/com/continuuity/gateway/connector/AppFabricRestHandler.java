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
import com.continuuity.common.utils.StackTraceUtil;
import com.continuuity.gateway.GatewayMetricsHelperWrapper;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.internal.app.services.legacy.FlowDefinitionImpl;
import com.continuuity.internal.app.services.legacy.FlowletDefinition;
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
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

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the Http request handler for the AppFabric rest connector. At this time it accepts GET, POST and PUT
 * requests. REST calls can be used to deploy an application, start or stop, or get the status of a flow, procedure
 * or a map reduce job. It also supports reading and changing the number of instances of a flowlet.
 * a Post of http://<hostname>:<port>/app with the jar file of the to be deployed app in the content of the request
 * can be used to deploy a new app.
 * a Get http://<hostname>:<port>/app/status returns the current deployment status
 * a Post
 */
public class AppFabricRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricRestHandler.class);

  private static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";
  private static final String APPFABRIC_SERVICE_NAME = "app.fabric.service";

  private static final Set<String> SUPPORTED_FLOW_OPERATIONS = Sets.newHashSet("stop", "start", "status");
  private static final Set<String> SUPPORTED_FLOWLET_OPERATIONS = Sets.newHashSet("instances");
  private static final Set<String> SUPPORTED_ENTITY_TYPES = Sets.newHashSet("flow", "procedure", "mapreduce");

  /**
   * The allowed methods for this handler.
   */
  private static final Set<HttpMethod> ALLOWED_HTTP_METHODS = Sets.newHashSet(HttpMethod.PUT, HttpMethod.POST,
                                                                              HttpMethod.GET);

  /**
   * Will help validate URL paths, authenticate and get a metrics helper
   */
  private AppFabricRestConnector connector;

  /**
   * The metrics object of this rest connector.
   */
  private final CMetrics metrics;

  // This is the prefix that all valid URLs must have.
  private final String pathPrefix;

  /**
   * Constructor requires the connector that created this.
   *
   * @param connector the connector that created this
   */
  AppFabricRestHandler(AppFabricRestConnector connector) {
    this.connector = connector;
    metrics = connector.getMetricsClient();
    pathPrefix = connector.getHttpConfig().getPathPrefix() + connector.getHttpConfig().getPathMiddle();
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
      LOG.trace("Request received: {} {}", method, requestUri);
    }
    GatewayMetricsHelperWrapper metricsHelper =
      new GatewayMetricsHelperWrapper(new MetricsHelper(this.getClass(), metrics, connector.getMetricsQualifier()),
                                      connector.getGatewayMetrics());

    try {
      // check whether the request's HTTP method is supported
      if (!ALLOWED_HTTP_METHODS.contains(method)) {
        LOG.trace("Received a {} request, which is not supported (path is + '{}')", method, path);
        respondNotAllowed(message.getChannel(), ALLOWED_HTTP_METHODS);
        metricsHelper.finish(BadRequest);
        return;
      }

      // ping doesn't need an auth token.
      if ("/ping".equals(requestUri) && HttpMethod.GET.equals(method)) {
        metricsHelper.setMethod("ping");
        respondToPing(message.getChannel(), request);
        metricsHelper.finish(Success);
        return;
      }

      // check that path begins with pathPrefix, usually '/app' unless configured differently
      if (!path.startsWith(pathPrefix)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Received a request with unkown path prefix (must be '" + pathPrefix + "' but received '"
                      + path + "'.");
        }
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        metricsHelper.finish(NotFound);
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
        LOG.info("No valid account information found");
        respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
        metricsHelper.finish(BadRequest);
        return;
      }

      AppFabricService.Client client = getAppFabricClient();
      Preconditions.checkArgument(client != null);

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));

      try {
        if (pathPrefix.equals(path)) {  // Post with plain '/app' to deploy an app
          if (request.getMethod() != HttpMethod.POST) {
            LOG.trace("Only Http Post method is supported for this operation.");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            metricsHelper.finish(BadRequest);
            return;
          }
          String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);
          if (archiveName == null || archiveName.isEmpty()) {
            LOG.trace("Archive name was not available in the header");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            metricsHelper.finish(BadRequest);
            return;
          }

          ChannelBuffer content = request.getContent();
          if (content == null) {
            LOG.trace("No body passed from client");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
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

        // from here on, path is either
        // /app/status
        // or
        // /app/<app-id>/<flow-type>/<flow id>?op=<op> with <op> as start, stop, status
        // or
        // /app/<app-id>/flow/<flow id>/<flowlet id>?op=instances

        // remove pathPrefix '/app' and separator '/', then split path into elements
        String[] pathElements = path.substring(pathPrefix.length() + 1).split("/");

        if (pathElements.length == 1) {
          if ("status".equals(pathElements[0])) {
            if (request.getMethod() != HttpMethod.GET) {
              LOG.trace("Only Http Get method is supported for this operation.");
              respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
              metricsHelper.finish(BadRequest);
              return;
            }
            ResourceIdentifier rIdentifier = new ResourceIdentifier(accountId, "no-app", "no-res", 1);
            DeploymentStatus status = client.dstatus(token, rIdentifier);
            byte[] response =
              getJsonStatus(status.getOverall(), status.getMessage()).toString().getBytes(Charset.forName("UTF-8"));
            respondJson(message.getChannel(), request, HttpResponseStatus.OK, response);
            metricsHelper.finish(Success);
            return;
          } else {
            LOG.trace("Received unsupported request URL.");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            metricsHelper.finish(BadRequest);
            return;
          }
        }

        if (pathElements.length != 3 && pathElements.length != 4) {
          LOG.trace("Unsupported number of path elements in request URL.");
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
          metricsHelper.finish(BadRequest);
          return;
        }

        String appId = pathElements[0];
        String entityType = pathElements[1];
        String flowId = pathElements[2];
        FlowIdentifier flowIdent = new FlowIdentifier(accountId, appId, flowId, 1);

        // making sure entityType is among supported flow types
        if (!SUPPORTED_ENTITY_TYPES.contains(entityType)) {
          LOG.trace("Unsupported entity type {} in request URL.", entityType);
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
          metricsHelper.finish(BadRequest);
          return;
        }

        if ("flow".equals(entityType)) {
          flowIdent.setType(EntityType.FLOW);

        } else if ("procedure".equals(entityType)) {
          flowIdent.setType(EntityType.QUERY);

        } else if ("mapreduce".equals(entityType)) {
          flowIdent.setType(EntityType.MAPREDUCE);
        }

        if (pathElements.length == 3) { //path is /app/<app-id>/<flow-type>/<flow id> ?
          handleFlowOperation(message, request, metricsHelper, client, token, flowIdent, decoder.getParameters());

        } else if (pathElements.length == 4) { //path is /app/<app-id>/<flow-type>/<flow id>/<flowlet id> ?
          handleFlowletOperation(message, request, metricsHelper, client, token, flowIdent, pathElements[3],
                                 decoder.getParameters());
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
      LOG.error("Exception caught for connector '" + this.connector.getName() + "'. ", e.getCause());
      metricsHelper.finish(Error);
      if (message.getChannel().isOpen()) {
        respondError(message.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        message.getChannel().close();
      }
    }
  }

  private void handleFlowOperation(MessageEvent message, HttpRequest request, GatewayMetricsHelperWrapper metricsHelper,
                                   AppFabricService.Client client, AuthToken token, FlowIdentifier flowIdent,
                                   Map<String, List<String>> parameters)
    throws TException, AppFabricServiceException {

    // looking for ?op=start, ?op=stop or ?op=status parameter in request
    List<String> operations = parameters.get("op");
    if (operations == null || operations.size() == 0) {
      LOG.trace("Parameter 'op' is missing.");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);
      return;
    } else if (operations.size() > 1) {
      LOG.trace("More than one 'op' parameter specified.");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);
      return;
    } else if (operations.size() == 1 && !SUPPORTED_FLOW_OPERATIONS.contains(operations.get(0))) {
      LOG.trace("Unsupported flow operation {}.", operations.get(0));
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);
      return;
    }

    String flowOperation = operations.get(0);

    // only HttpMethod.POST is supported for start and stop operations
    if (("start".equals(flowOperation) || "stop".equals(flowOperation)) && request.getMethod() != HttpMethod.POST) {
      LOG.trace("Only Http Post method is supported for this operation.");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);
      return;
    }

    // only HttpMethod.GET is supported for status operation
    if ("status".equals(flowOperation) && request.getMethod() != HttpMethod.GET) {
      LOG.trace("Only Http Get method is supported for this operation.");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);
      return;
    }

    // ignoring that flow might be running already when starting flow; or has been stopped before trying to stop flow
    if ("start".equals(flowOperation)) {
      client.start(token, new FlowDescriptor(flowIdent, ImmutableMap.<String, String>of()));
      // returning success, application needs to retrieve status of flow to verify that flow is actually running
      respondSuccess(message.getChannel(), request);
      metricsHelper.finish(Success);

    } else if ("stop".equals(flowOperation)) {
      client.stop(token, flowIdent);
      // returning success, application needs to retrieve status of flow to verify that flow has been stopped
      respondSuccess(message.getChannel(), request);
      metricsHelper.finish(Success);

    } else if ("status".equals(flowOperation)) {
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
                                      AuthToken token, FlowIdentifier flowIdent, String flowletId,
                                      Map<String, List<String>> parameters)
    throws TException, AppFabricServiceException {

    // looking for ?op=instances
    List<String> operations = parameters.get("op");
    if (operations == null || operations.size() == 0) {
      LOG.trace("Parameter 'op' is missing.");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);
      return;
    } else if (operations.size() > 1) {
      LOG.trace("More than one 'op' parameter specified.");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);
      return;
    } else if (operations.size() == 1 && !SUPPORTED_FLOWLET_OPERATIONS.contains(operations.get(0))) {
      LOG.trace("Unsupported flowlet operation {}.", operations.get(0));
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);
      return;
    }

    if (request.getMethod() == HttpMethod.GET) { // retrieve number of flowlet instances
      String flowDefJson = client.getFlowDefinition(flowIdent);
      if (flowDefJson == null) {
        LOG.error("Failed to get number of flowlet instances.");
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
      LOG.trace("Flowlet {} does not exist.", flowletId);
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);

    } else if (request.getMethod() == HttpMethod.PUT) {  // set number of flowlet instances
      // looking for Json string {"instances":<number>} in content of request body
      Type stringMapType = new TypeToken<Map<String, String>>() {}.getType();
      Map<String, String> valueMap;
      try {
        InputStreamReader reader =
          new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
        valueMap = new Gson().fromJson(reader, stringMapType);
        short numInstances = Short.parseShort(valueMap.get("instances"));
        if (numInstances < 1) {
          LOG.trace("Number of specified flowlet instances has to be greather than 0.");
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
          metricsHelper.finish(BadRequest);
        } else {
          client.setInstances(token, flowIdent, flowletId, numInstances);
          LOG.trace("Changed number of flowlet instances to {}.", numInstances);
          respondSuccess(message.getChannel(), request);
          metricsHelper.finish(Success);
        }
        return;
      } catch (Exception e) {
        // failed to parse json, so respond with bad request
        LOG.trace("Failed to read body as json: {}.", e.getMessage());
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        metricsHelper.finish(BadRequest);
        return;
      }

    } else {
      LOG.trace("Only Http Put and Get methods are supported by this operation.");
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      metricsHelper.finish(BadRequest);
      return;
    }
  }

  private void respondJson(Channel channel, HttpRequest request, HttpResponseStatus status, byte[] jsonContent) {
    Map<String, String> headers = Maps.newHashMap();
    headers.put(HttpHeaders.Names.CONTENT_TYPE, "application/json");
    respond(channel, request, status, headers, jsonContent);
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
}
