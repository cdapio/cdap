package com.continuuity.gateway.accessor;

import com.continuuity.app.services.ActiveFlow;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.FlowStatus;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.common.service.ServerException;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.GatewayMetricsHelperWrapper;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.metrics2.thrift.Counter;
import com.continuuity.metrics2.thrift.CounterRequest;
import com.continuuity.metrics2.thrift.FlowArgument;
import com.continuuity.metrics2.thrift.MetricsFrontendService;
import com.continuuity.weave.discovery.Discoverable;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.NotFound;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the metrics and status REST API.
 * <p/>
 * At this time it only accepts GET requests, which it forwards as Thrift
 * calls to the Flow and Metricvs services.
 * Examples of well-formed reqeuests:
 * <PRE>
 * http://gateway:port/rest-status/app-id/flow-id/status
 * http://gateway:port/rest-status/app-id/flow-id/metrics?counter=cntA,cntB
 * </PRE>
 */
public class MonitorRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
    .getLogger(MonitorRestHandler.class);

  /**
   * The allowed methods for this handler.
   */
  Set<HttpMethod> allowedMethods = Collections.singleton(
    HttpMethod.GET);

  /**
   * All the paths have to be of the form
   * http://host:port&lt;prefix>&lt;appid>/&lt;flowid>?status
   * or
   * http://host:port&lt;prefix>&lt;appid>/&lt;flowid>?metrics&amp;counters=...
   * example:
   * <PRE>GET http://g.c.c/rest-status/myapp/flow1?status</PRE> or
   * <PRE>GET http://g.c.c/rest-status/myapp/flow1?metrics&counters=c1,c2</PRE>
   */
  private String pathPrefix;

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private MonitorRestAccessor accessor;

  /**
   * The metrics object of the rest accessor.
   */
  private CMetrics metrics;

  private final EndpointStrategy flowEndpoints;

  private final EndpointStrategy metricsEndpoints;

  /**
   * Constructor requires the accessor that created this.
   *
   * @param accessor the accessor that created this
   */
  MonitorRestHandler(MonitorRestAccessor accessor) {
    this.accessor = accessor;
    this.metrics = accessor.getMetricsClient();
    this.pathPrefix =
        accessor.getHttpConfig().getPathPrefix() +
            accessor.getHttpConfig().getPathMiddle();
    flowEndpoints = new RandomEndpointStrategy(accessor.getDiscoveryServiceClient()
                                                 .discover(Constants.FLOW_SERVICE_NAME));
    metricsEndpoints = new RandomEndpointStrategy(accessor.getDiscoveryServiceClient()
                                                    .discover(Constants.METRICS_SERVICE_NAME));
  }

  // a metrics thrift client for every thread
  ThreadLocal<MetricsFrontendService.Client> metricsClients =
    new ThreadLocal<MetricsFrontendService.Client>();

  // a flow thrift client for every thread
  ThreadLocal<AppFabricService.Client> flowClients =
    new ThreadLocal<AppFabricService.Client>();

  /**
   * generic method to discover a thrift service and start up the
   * thrift transport and protocol layer.
   */
  private TProtocol getThriftProtocol(String serviceName, EndpointStrategy endpointStrategy) throws ServerException {
    Discoverable endpoint = endpointStrategy.pick();
    if (endpoint == null) {
      String message = String.format("Service '%s' is not registered in discovery service.", serviceName);
      LOG.error(message);
      throw new ServerException(message);
    }
    TTransport transport = new TFramedTransport(
        new TSocket(endpoint.getSocketAddress().getHostName(), endpoint.getSocketAddress().getPort()));
    try {
      transport.open();
    } catch (TTransportException e) {
      String message = String.format("Unable to connect to thrift service %s at %s. Reason: %s",
                                     serviceName, endpoint.getSocketAddress(), e.getMessage());
      LOG.error(message);
      throw new ServerException(message, e);
    }
    // now try to connect the thrift client
    return new TBinaryProtocol(transport);
  }

  /**
   * obtain a metrics thrift client from the thread-local, if necessary create
   * and connect the client (when this thread needs it for the first time).
   *
   * @return A connected metrics client
   * @throws ServerException if service discovery or connecting to the
   *                         service fails.
   */
  private MetricsFrontendService.Client getMetricsClient()
      throws ServerException {
    if (metricsClients.get() == null || !metricsClients.get().getInputProtocol().getTransport().isOpen()) {
      TProtocol protocol = getThriftProtocol(Constants.METRICS_SERVICE_NAME, metricsEndpoints);
      MetricsFrontendService.Client client = new MetricsFrontendService.Client(protocol);
      metricsClients.set(client);
    }
    return metricsClients.get();
  }

  /**
   * obtain a flow thrift client from the thread-local, if necessary create
   * and connect the client (when this thread needs it for the first time).
   *
   * @return A connected flow client
   * @throws ServerException if service discovery or connecting to the
   *                         service fails.
   */
  private AppFabricService.Client getFlowClient() throws ServerException {
    if (flowClients.get() == null || !flowClients.get().getInputProtocol().getTransport().isOpen()) {
      TProtocol protocol = getThriftProtocol(Constants.FLOW_SERVICE_NAME, flowEndpoints);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      flowClients.set(client);
    }
    return flowClients.get();
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
      // only GET is supported for now
      if (method != HttpMethod.GET) {
        LOG.trace("Received a " + method + " request, which is not supported");
        respondNotAllowed(message.getChannel(), allowedMethods);
        helper.finish(BadRequest);
        return;
      }

      QueryStringDecoder decoder = new QueryStringDecoder(uri);
      Map<String, List<String>> parameters = decoder.getParameters();
      String path = decoder.getPath();

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
      if ("/ping".equals(path)) {
        helper.setMethod("ping");
        respondToPing(message.getChannel(), request);
        helper.finish(Success);
        return;
      }

      // entry point for internal continuuity metrics monitoring
      if ("/flowmetrics".equals(path)) {
        helper.setMethod("flowmetrics");

        StringBuilder resp = new StringBuilder();
        boolean first = true;

        Map<String, Integer> statusmetrics = new HashMap<String, Integer>();
        // initialize zeros for the minimal set of statuses we always want to return
        statusmetrics.put("RUNNING", 0);
        statusmetrics.put("STOPPED", 0);
        statusmetrics.put("DEPLOYED", 0);
        statusmetrics.put("FAILED", 0);
        statusmetrics.put("STARTING", 0);
        statusmetrics.put("STOPPING", 0);
        AppFabricService.Client flowClient = this.getFlowClient();
        List<ActiveFlow> activeFlows = flowClient.getFlows(accountId);
        //iterate through flows, build up response string
        for (ActiveFlow activeFlow : activeFlows) {
          // increment general status metric
          if (!"".equals(activeFlow.getCurrentState())) {
            int count = statusmetrics.containsKey(activeFlow.getCurrentState())
              ? statusmetrics.get(activeFlow.getCurrentState()) : 0;
            statusmetrics.put(activeFlow.getCurrentState(), count + 1);
          }
          // get flow metrics for this flow
          MetricsFrontendService.Client metricsClient = this.getMetricsClient();
          CounterRequest counterRequest = new CounterRequest(
            new FlowArgument(accountId, activeFlow.getApplicationId(),
                             activeFlow.getFlowId()));
          List<String> counterNames = parameters.get("counter");
          if (counterNames != null) {
            counterRequest.setName(counterNames);
          }
          List<Counter> counters = metricsClient.getCounters(counterRequest);
          // append this flow's metrics to response
          for (Counter counter : counters) {
            if (first) {
              first = false;
            } else {
              resp.append(',');
            }
            if (counter.isSetQualifier()) {
              resp.append("flows.").append(activeFlow.getApplicationId()).append('.');
              resp.append(activeFlow.getFlowId()).append('.');
              resp.append(counter.getQualifier()).append(".");
            }
            resp.append(counter.getName()).append('=').append(counter.getValue());
          }
        }

        // append general flow status metrics to response
        for (Map.Entry<String, Integer> entry : statusmetrics.entrySet()) {
          String key = entry.getKey();
          int value = entry.getValue();
          if (first) {
            first = false;
          } else {
            resp.append(',');
          }
          resp.append("flows.").append(key.toLowerCase()).append('=').append(value);
        }

        respondSuccess(message.getChannel(), request, resp.toString().getBytes());
        helper.finish(Success);
        return;
      }

      // parse and verify the url path
      String appid = null, flowid = null, query = null;
      // valid paths are <prefix>/service/method?param=value&...
      if (path.startsWith(this.pathPrefix)) {
        int pos1 = path.indexOf("/", this.pathPrefix.length());
        if (pos1 > this.pathPrefix.length()) { // appid not empty
          int pos2 = path.indexOf("/", pos1 + 1);
          if (pos2 > pos1 + 1) { // flowid not empty
            int pos3 = path.indexOf("/", pos2 + 1);
            if (pos3 < 0 && path.length() > pos2) { // method not empty, no more /
              appid = path.substring(this.pathPrefix.length(), pos1);
              flowid = path.substring(pos1 + 1, pos2);
              query = path.substring(pos2 + 1);
            }
          }
        }
      }
      // is the path well-formed (prefix/app/flow/query?...)
      if (appid == null) {
        helper.finish(BadRequest);
        LOG.trace("Received a request with unsupported path " + uri);
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }
      // is the query supported (only status or metrics right now)
      if (!("status".equals(query) || "metrics".equals(query))) {
        helper.finish(BadRequest);
        LOG.trace("Received a request with unsupported query " + query);
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
        return;
      }
      helper.setMethod("query");

      if ("status".equals(query)) {
        AppFabricService.Client flowClient = this.getFlowClient();
        FlowStatus status = flowClient.status(new AuthToken(),
                                              new FlowIdentifier(accountId, appid, flowid, -1));
        String value = status.getStatus();
        respondSuccess(message.getChannel(), request, value.getBytes());
        helper.finish(Success);
      } else if ("metrics".equals(query)) {
        MetricsFrontendService.Client metricsClient = this.getMetricsClient();
        CounterRequest counterRequest = new CounterRequest(
          new FlowArgument(accountId, appid, flowid));
        List<String> counterNames = parameters.get("counter");
        if (counterNames != null) {
          counterRequest.setName(counterNames);
        }
        List<Counter> counters = metricsClient.getCounters(counterRequest);
        StringBuilder str = new StringBuilder();
        boolean first = true;
        for (Counter counter : counters) {
          if (first) {
            first = false;
          } else {
            str.append(',');
          }
          if (counter.isSetQualifier()) {
            str.append(counter.getQualifier()).append(".");
          }
          str.append(counter.getName()).append('=').append(counter.getValue());
        }
        respondSuccess(message.getChannel(), request, str.toString().getBytes());
        helper.finish(Success);

      } else {
        // this should not happen because we checked above -> internal error
        helper.finish(Error);
        respondError(message.getChannel(),
                     HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
    } catch (Exception e) {
      LOG.error("Exception caught for connector '" +
                  this.accessor.getName() + "'. ", e);
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
    LOG.error("Exception caught for connector '" + this.accessor.getName() + "'. ", e.getCause());
    if (e.getChannel().isOpen()) {
      respondError(e.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }
}
