package com.continuuity.gateway.connector;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DeploymentStatus;
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
import com.continuuity.weave.discovery.Discoverable;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
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
import java.util.ArrayList;
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
  private static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";
  private static final String APPFABRIC_SERVICE_NAME = "app.fabric.service";

  private static final Set<String> SUPPORTED_FLOW_OPERATIONS = Sets.newHashSet("stop", "start", "status");

  /**
   * The allowed methods for this handler.
   */
  private static final Set<HttpMethod> ALLOWED_HTTP_METHODS = Sets.newHashSet(
    HttpMethod.PUT,
    HttpMethod.POST,
    HttpMethod.GET);

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private AppFabricRestConnector connector;

  /**
   * The metrics object of the rest connector.
   */
  private final CMetrics metrics;

  /**
   * Constructor requires the connector that created this.
   *
   * @param connector the connector that created this
   */
  AppFabricRestHandler(AppFabricRestConnector connector) {
    this.connector = connector;
    this.metrics = connector.getMetricsClient();
  }

  @Override
  public void messageReceived(ChannelHandlerContext context, MessageEvent message) throws Exception {
    HttpRequest request = (HttpRequest) message.getMessage();

    GatewayMetricsHelperWrapper helper =
      new GatewayMetricsHelperWrapper(new MetricsHelper(this.getClass(), metrics, connector.getMetricsQualifier()),
                                      connector.getGatewayMetrics());

    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    String path = decoder.getPath();

    // Ping doesn't need a auth token.
    if ("/ping".equals(path)) {
      helper.setMethod("ping");
      respondToPing(message.getChannel(), request);
      helper.finish(Success);
      return;
    }

    if (!connector.getAuthenticator().authenticateRequest(request)) {
      respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
      helper.finish(BadRequest);
      return;
    }

    String accountId = connector.getAuthenticator().getAccountId(request);
    if (accountId == null || accountId.isEmpty()) {
      respondError(message.getChannel(), HttpResponseStatus.FORBIDDEN);
      helper.finish(BadRequest);
      return;
    }

    try {
      if (!ALLOWED_HTTP_METHODS.contains(request.getMethod())) {
        LOG.trace("Received Unsupported http request method " + request.getMethod().getName());
        respondNotAllowed(message.getChannel(), ALLOWED_HTTP_METHODS);
        helper.finish(BadRequest);
        return;
      }

      AppFabricService.Client client = getAppFabricClient();
      Preconditions.checkArgument(client != null);

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));

      try {

        if (path.contains("/apps/status")) {
          ResourceIdentifier rIdentifier = new ResourceIdentifier(accountId, "no-app", "no-res", 1);
          DeploymentStatus status = client.dstatus(token, rIdentifier);
          Map<String, String> headers = Maps.newHashMap();
          headers.put(HttpHeaders.Names.CONTENT_TYPE, "application/json");
          respond(message.getChannel(), request, HttpResponseStatus.OK, headers,
                  getJsonStatus(status.getOverall(),
                                status.getMessage()).toString().getBytes(Charset.forName("UTF-8")));
          helper.finish(Success);

        } else if ("/app".equals(path)) {
          String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);
          if (archiveName == null || archiveName.isEmpty()) {
            LOG.trace("Archive name was not available in the header");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            helper.finish(BadRequest);
            return;
          }

          ChannelBuffer content = request.getContent();
          if (content == null) {
            LOG.trace("No body passed from client");
            respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
            helper.finish(BadRequest);
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

        } else if (path.startsWith("/app/")) {

          //path is either /app/<app-id>/<flow id> or /app/<app-id>/<flow id>/<flowlet id>
          String[] pathElements = path.substring(5).split("/"); //remove prefix '/app/' then split

          if (pathElements.length < 2 || pathElements.length > 3) {
            respondBadRequest(message, request, helper, " incorrect URL");
            return;
          }
          String appid = pathElements[0];
          String flowId = pathElements[1];
          FlowIdentifier flowIdent = new FlowIdentifier(accountId, appid, flowId, -1);

          if (pathElements.length == 2) { //pathElements are <app-id> and <flow id> ?

            //only HttpMethod.POST is supported
            if (request.getMethod() != HttpMethod.POST) {
              respondBadRequest(message, request, helper,
                                " only Http method " + HttpMethod.POST.getName() + " is supported");
              return;
            }

            //looking for ?op=start or ?op=stop parameters in request url

            List<String> operations = decoder.getParameters().get("op");
            if (operations == null || operations.size() == 0) {
              respondBadRequest(message, request, helper, "no 'op' parameter specified");
              return;
            } else if (operations.size() > 1) {
              respondBadRequest(message, request, helper, "more than one 'op' parameter specified");
              return;
            } else if (operations.size() == 1 && !SUPPORTED_FLOW_OPERATIONS.contains(operations.get(0))) {
              respondBadRequest(message, request, helper, "unsupported 'op' parameter specified");
              return;
            }

            String op = operations.get(0);
            if ("start".equals(op)) {
              client.start(token, new FlowDescriptor(flowIdent,new ArrayList<String>()));
            } else if ("stop".equals(op)) {
              client.stop(token, flowIdent);
            } else if ("status".equals(op)) {
              FlowStatus flowStatus = client.status(token, flowIdent);
              if (flowStatus == null) {
                respondBadRequest(message, request, helper, "failed to get flow status");
              }
              byte[] response = flowStatus.toString().getBytes(Charsets.UTF_8);
              respondSuccess(message.getChannel(), request, response);
            }

          } else if (pathElements.length == 3) { //pathElements are <app-id>, <flow id> and <flowlet id> ?

            //String flowDef = client.getFlowDefinition(flowIdent);

            //only HttpMethod.PUT is supported
            if (request.getMethod() != HttpMethod.PUT) {
              respondBadRequest(message, request, helper,
                                " only Http method " + HttpMethod.PUT.getName() + " is supported");
              return;
            }
            String flowletId = pathElements[2];

            //looking for {"instances":<number>} in content of request body
            Type stringMapType = new TypeToken<Map<String, String>>() {}.getType();
            Map<String, String> valueMap;
            try {
              InputStreamReader reader =
                new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
              valueMap = new Gson().fromJson(reader, stringMapType);
            } catch (Exception e) {
              // failed to parse json, that is a bad request
              respondBadRequest(message, request, helper, "failed to read body as json: " + e.getMessage());
              return;
            }

            short numInstances = Short.parseShort(valueMap.get("instances"));
            Preconditions.checkArgument(numInstances > 0);
            client.setInstances(token, flowIdent, flowletId, numInstances);
          }
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
