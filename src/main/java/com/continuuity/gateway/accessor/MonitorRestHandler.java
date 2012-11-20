package com.continuuity.gateway.accessor;

import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.flow.manager.stubs.DelegationToken;
import com.continuuity.flow.manager.stubs.FlowIdentifier;
import com.continuuity.flow.manager.stubs.FlowService;
import com.continuuity.flow.manager.stubs.FlowStatus;
import com.continuuity.gateway.Constants;
import com.continuuity.gateway.util.MetricsHelper;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.metrics2.thrift.Counter;
import com.continuuity.metrics2.thrift.CounterRequest;
import com.continuuity.metrics2.thrift.FlowArgument;
import com.continuuity.metrics2.thrift.MetricsFrontendService;
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

import java.util.List;
import java.util.Map;

import static com.continuuity.gateway.util.MetricsHelper.Status.BadRequest;
import static com.continuuity.gateway.util.MetricsHelper.Status.Error;
import static com.continuuity.gateway.util.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the metrics and status REST API.
 * <p>
 * At this time it only accepts GET requests, which it forwards as Thrift
 * calls to the Flow and Metricvs services.
 * Examples of well-formed reqeuests:
 * <PRE>
 *   http://gateway:port/rest-status/app-id/flow-id/status
 *   http://gateway:port/rest-status/app-id/flow-id/metrics?counter=cntA,cntB
 * </PRE>
 *
 */
public class MonitorRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(MonitorRestHandler.class);

  /**
   * The allowed methods for this handler
   */
  HttpMethod[] allowedMethods = {
      HttpMethod.GET
  };

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
   * The metrics object of the rest accessor
   */
  private CMetrics metrics;

  /**
   * Constructor requires the accessor that created this
   *
   * @param accessor the accessor that created this
   */
  MonitorRestHandler(MonitorRestAccessor accessor) {
    this.accessor = accessor;
    this.metrics = accessor.getMetricsClient();
    this.pathPrefix =
        accessor.getHttpConfig().getPathPrefix() +
            accessor.getHttpConfig().getPathMiddle();
  }

  // a metrics thrift client for every thread
  ThreadLocal<MetricsFrontendService.Client> metricsClients =
      new ThreadLocal<MetricsFrontendService.Client>();

  // a flow thrift client for every thread
  ThreadLocal<FlowService.Client> flowClients =
      new ThreadLocal<FlowService.Client>();

  /**
   * generic method to discover a thrift service and start up the
   * thrift transport and protocol layer
   */
  private TProtocol getThriftProtocol(String serviceName)
      throws ServerException{

    ImmutablePair<String, Integer> addr =
        this.accessor.getServiceDiscovery().getServiceAddress(serviceName);
    // may return null
    if (addr == null) {
      String message = String.format("Service '%s' is not registered in " +
          "discovery service.", serviceName);
      LOG.error(message);
      throw new ServerException(message);
    }
    TTransport transport = new TFramedTransport(
        new TSocket(addr.getFirst(), addr.getSecond()));
    try {
      transport.open();
    } catch (TTransportException e) {
      String message = String.format("Unable to connect to thrift " +
          "service %s at %s:%d. Reason: %s", serviceName, addr.getFirst(),
          addr.getSecond(), e.getMessage());
      LOG.error(message);
      throw new ServerException(message, e);
    }
    // now try to connect the thrift client
    return new TBinaryProtocol(transport);
  }

  /**
   * obtain a metrics thrift client from the thread-local, if necessary create
   * and connect the client (when this thread needs it for the first time)
   * @return A connected metrics client
   * @throws ServerException if service discovery or connecting to the
   * service fails.
   */
  private MetricsFrontendService.Client getMetricsClient()
      throws ServerException {
    if (metricsClients.get() == null ||
        !metricsClients.get().getInputProtocol().getTransport().isOpen()) {
      TProtocol protocol = getThriftProtocol(Constants.metricsServiceName);
      MetricsFrontendService.Client client = new
          MetricsFrontendService.Client(protocol);
      metricsClients.set(client);
    }
    return metricsClients.get();
  }

  /**
   * obtain a flow thrift client from the thread-local, if necessary create
   * and connect the client (when this thread needs it for the first time)
   * @return A connected flow client
   * @throws ServerException if service discovery or connecting to the
   * service fails.
   */
  private FlowService.Client getFlowClient() throws ServerException {
    if (flowClients.get() == null ||
      !flowClients.get().getInputProtocol().getTransport().isOpen()) {
        TProtocol protocol = getThriftProtocol(Constants.flowServiceName);
      FlowService.Client client = new FlowService.Client(protocol);
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
    MetricsHelper helper = new MetricsHelper(
        this.getClass(), this.metrics, this.accessor.getName());

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
        FlowService.Client flowClient = this.getFlowClient();
        FlowStatus status = flowClient.status(new DelegationToken(),
            new FlowIdentifier(Constants.defaultAccount, appid, flowid, -1));
        String value = status.getStatus();
        respondSuccess(message.getChannel(), request, value.getBytes());
        helper.finish(Success);
      } else if ("metrics".equals(query)) {
        MetricsFrontendService.Client metricsClient = this.getMetricsClient();
        CounterRequest counterRequest = new CounterRequest(
            new FlowArgument(Constants.defaultAccount, appid, flowid));
        List<String> counterNames = parameters.get("counter");
        if (counterNames != null) {
          counterRequest.setName(counterNames);
        }
        List<Counter> counters = metricsClient.getCounters(counterRequest);
        StringBuilder str = new StringBuilder();
        boolean first = true;
        for (Counter counter : counters) {
          if (first) first = false; else str.append(',');
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
    MetricsHelper.meterError(metrics, this.accessor.getName());
    LOG.error("Exception caught for connector '" +
        this.accessor.getName() + "'. ", e.getCause());
    if(e.getChannel().isOpen()) {
      respondError(e.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }
}
