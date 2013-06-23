package com.continuuity.gateway.accessor;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.Id;
import com.continuuity.app.program.RunRecord;
import com.continuuity.app.program.Type;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricsHelper;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.gateway.GatewayMetricsHelperWrapper;
import com.continuuity.gateway.util.NettyRestHandler;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.continuuity.metadata.thrift.Query;
import com.continuuity.metadata.thrift.Stream;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.apache.thrift.TException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;
import static com.continuuity.common.metrics.MetricsHelper.Status.Error;
import static com.continuuity.common.metrics.MetricsHelper.Status.Success;

/**
 * This is the http request handler for the system stats REST API.
 * <p/>
 * Example of well-formed reqeuest:
 * <PRE>
 * http://localhost:10006/systemstats
 * </PRE>
 */
public class SystemStatsRestHandler extends NettyRestHandler {

  private static final Logger LOG = LoggerFactory
    .getLogger(SystemStatsRestHandler.class);

  private static final MetricsFormatter METRICS_FORMATTER = new SimpleMetricsFormatter();

  /**
   * The allowed methods for this handler.
   */
  private static final Set<HttpMethod> allowedMethods = Sets.newHashSet(HttpMethod.GET);

  /**
   * Formats metrics.
   */
  public static interface MetricsFormatter {
    String format(Map<String, ?> metrics);
  }

  /**
   * Will help validate URL paths, and also has the name of the connector and
   * the data fabric executor.
   */
  private SystemStatsRestAccessor accessor;

  /**
   * The metrics object of the rest accessor.
   */
  private CMetrics metrics;

  /**
   * Constructor requires the accessor that created this.
   *
   * @param accessor the accessor that created this
   */
  SystemStatsRestHandler(SystemStatsRestAccessor accessor) {
    this.accessor = accessor;
    this.metrics = accessor.getMetricsClient();
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
      String path = decoder.getPath();

      // is this a ping? (http://gw:port/ping) if so respond OK and done
      if ("/ping".equals(path)) {
        helper.setMethod("ping");
        respondToPing(message.getChannel(), request);
        helper.finish(Success);
        return;
      }

      // entry point for internal continuuity metrics monitoring
      if ("/systemstats".equals(path)) {
        helper.setMethod("systemstats");

        // "root" is actually mock here. Replace with real root (sys admin) account
        Collection<String> accounts = accessor.getMetaDataStore().listAccounts(new OperationContext("root"));

        // Using tree map to make metrics sorted - just for easier "eye-balling"
        Map<String, Long> stats = Maps.newTreeMap();

        // User apps metrics
        for (String accountId : accounts) {
          collectStatsForAccount(accountId, stats);
        }

        // System metrics: gateway
        stats.putAll(accessor.getGatewayMetrics().getMetrics());

        respondSuccess(message.getChannel(), request, METRICS_FORMATTER.format(stats).getBytes());
        helper.finish(Success);
      } else {
        respondError(message.getChannel(), HttpResponseStatus.NOT_FOUND);
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

  private void collectStatsForAccount(String accountId, Map<String, Long> stats)
    throws MetadataServiceException, TException, OperationException {
    Account account = new Account(accountId);

    List<Application> applications = accessor.getMetadataService().getApplications(account);
    inc(stats, "application.count", applications.size());

    List<Dataset> datasets = accessor.getMetadataService().getDatasets(account);
    inc(stats, "dataset.count", datasets.size());

    List<Stream> streams = accessor.getMetadataService().getStreams(account);
    inc(stats, "stream.count", streams.size());

    List<Flow> flows = accessor.getMetadataService().getFlows(accountId);
    inc(stats, "flow.count", flows.size());

    List<Query> procedures = accessor.getMetadataService().getQueries(account);
    inc(stats, "procedure.count", procedures.size());

    collectRunHistory(accountId, stats);
  }

  private void collectRunHistory(String accountId, Map<String, Long> stats) throws OperationException {
    // note: this could be quite expensive to collect
    Table<Type, Id.Program, List<RunRecord>> runHistory = accessor.getStore().getAllRunHistory(new Id.Account(accountId));
    for (Table.Cell<Type, Id.Program, List<RunRecord>> run : runHistory.cellSet()) {
      for (RunRecord runRecord : run.getValue()) {
        Type programType = run.getRowKey();
        inc(stats, "run.start.count", 1);
        inc(stats, "run.start.program_type." + programType.name() + ".count", 1);
        if (runRecord.getStopTs() > 0) {
          inc(stats, "run.stop.count", 1);
          inc(stats, "run.stop.end_status." + runRecord.getEndStatus() + ".count", 1);
          inc(stats, "run.stop.program_type." + programType.name() + ".count", 1);
          inc(stats, "run.stop.program_type.end_status." +
            programType.name() + "." + runRecord.getEndStatus() + ".count", 1);
        }
      }
    }
  }

  private static void inc(Map<String, Long> stats, String key, long delta) {
    Long existingValue = stats.get(key);
    if (existingValue == null) {
      stats.put(key, delta);
    } else {
      stats.put(key, existingValue + delta);
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

  /**
   * Formats metrics so that each is place on one line with key and value separated with space.
   */
  public static final class SimpleMetricsFormatter implements MetricsFormatter {
    @Override
    public String format(Map<String, ?> metrics) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Map.Entry<String, ?> metric : metrics.entrySet()) {
        if (!first) {
          sb.append("\n");
        }
        sb.append(metric.getKey()).append(" ").append(metric.getValue().toString());
        first = false;
      }
      return sb.toString();
    }
  }
}
