package com.continuuity.metrics2.frontend;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.context.LoggingContextHelper;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.filter.FilterParser;
import com.continuuity.logging.read.Callback;
import com.continuuity.logging.read.LogEvent;
import com.continuuity.logging.read.LogReader;
import com.continuuity.metrics2.thrift.Counter;
import com.continuuity.metrics2.thrift.CounterRequest;
import com.continuuity.metrics2.thrift.MetricsFrontendService;
import com.continuuity.metrics2.thrift.MetricsServiceException;
import com.continuuity.metrics2.thrift.Points;
import com.continuuity.metrics2.thrift.TEntityType;
import com.continuuity.metrics2.thrift.TLogResult;
import com.continuuity.metrics2.thrift.TimeseriesRequest;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.continuuity.logging.context.LoggingContextHelper.EntityType;

/**
 * MetricsService provides a readonly service for metrics.
 * It's a implementation that reads from the SQL supported DB.
 */
public class MetricsFrontendServiceImpl
  implements MetricsFrontendService.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsFrontendServiceImpl.class);

  /**
   * Log reader. Marked as optional injection as currently log reader is piggy-backing on metricsfrontend service
   * We should remove it.
   * TODO: ENG-3102
   */
  @Inject(optional = true)
  private LogReader logReader;
  private final String logPattern;

  @Inject
  public MetricsFrontendServiceImpl(CConfiguration configuration) {
    this.logPattern = configuration.get(LoggingConfiguration.LOG_PATTERN, LoggingConfiguration.DEFAULT_LOG_PATTERN);
  }

  /**
   * Returns the requested counter for a given account, application, flow
   * & run. All the counter for the combination could be retrieved by specifying
   * ALL in the metric name.
   *
   * @param request
   */
  @Override
  public List<Counter> getCounters(CounterRequest request) throws MetricsServiceException, TException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * API to request time series data for a set of metrics.
   *
   * @param request
   */
  @Override
  public Points getTimeSeries(TimeseriesRequest request) throws MetricsServiceException, TException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void clear(String accountId, String applicationId) throws MetricsServiceException, TException {
  }

  /**
   * Resets the metrics for a given account.
   *
   * @param accountId for which the metrics needs to be set.
   * @throws MetricsServiceException thrown when there is issue with reseting
   * metrics.
   * @throws TException for Thrift level issues
   */
  @Override
  public void reset(String accountId) throws MetricsServiceException, TException {
  }

  @Override
  public List<String> getLog(final String accountId, final String applicationId,
                             final String flowId, int size)
    throws MetricsServiceException, TException {
    return Lists.newArrayList(
      Iterables.transform(
        getLogNext(accountId, applicationId, flowId, TEntityType.FLOW, -1, 200, ""), TLogResultConverter.getConverter()
      )
    );
  }

  /**
   * Converts TLogResult into String.
   */
  private static class TLogResultConverter implements Function<TLogResult, String> {
    private static final TLogResultConverter CONVERTER = new TLogResultConverter();

    public static TLogResultConverter getConverter() {
      return CONVERTER;
    }

    @Nullable
    @Override
    public String apply(@Nullable TLogResult input) {
      if (input == null) {
        return null;
      }
      return input.getLogLine();
    }
  }

  @Override
  public List<TLogResult> getLogNext(String accountId, String applicationId, String entityId, TEntityType entityType,
                                     long fromOffset, int maxEvents, String filterStr)
    throws MetricsServiceException, TException {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(accountId, applicationId,
                                                                           entityId, getEntityType(entityType));
    LogCallback logCallback = new LogCallback(maxEvents, logPattern);
    try {
      Filter filter = FilterParser.parse(filterStr);
      logReader.getLogNext(loggingContext, fromOffset, maxEvents, filter, logCallback);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return logCallback.getLogResults();
  }

  @Override
  public List<TLogResult> getLogPrev(String accountId, String applicationId, String entityId, TEntityType entityType,
                                     long fromOffset, int maxEvents, String filterStr)
    throws MetricsServiceException, TException {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(accountId, applicationId,
                                                                           entityId, getEntityType(entityType));
    LogCallback logCallback = new LogCallback(maxEvents, logPattern);
    try {
      Filter filter = FilterParser.parse(filterStr);
      logReader.getLogPrev(loggingContext, fromOffset, maxEvents, filter, logCallback);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return logCallback.getLogResults();
  }

  /**
   * Callback to handle log events from LogReader.
   */
  private static class LogCallback implements Callback {
    private final List<TLogResult> logResults;
    private final PatternLayout patternLayout;

    private final CountDownLatch doneLatch = new CountDownLatch(1);

    private LogCallback(int maxEvents, String logPattern) {
      logResults = Lists.newArrayListWithExpectedSize(maxEvents);

      ch.qos.logback.classic.Logger rootLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
      LoggerContext loggerContext = rootLogger.getLoggerContext();

      patternLayout = new PatternLayout();
      patternLayout.setContext(loggerContext);
      patternLayout.setPattern(logPattern);
    }

    @Override
    public void init() {
      patternLayout.start();
    }

    @Override
    public void handle(LogEvent event) {
      logResults.add(new TLogResult(patternLayout.doLayout(event.getLoggingEvent()), event.getOffset()));
    }

    @Override
    public void close() {
      patternLayout.stop();
      doneLatch.countDown();
    }

    public List<TLogResult> getLogResults() {
      try {
        doneLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return Collections.unmodifiableList(logResults);
    }
  }

  private EntityType getEntityType(TEntityType tEntityType) {
    switch (tEntityType) {
      case FLOW:
        return EntityType.FLOW;
      case PROCEDURE:
        return EntityType.PROCEDURE;
      case MAP_REDUCE:
        return EntityType.MAP_REDUCE;
      default:
        throw new IllegalArgumentException(String.format("Illegal TEntityType %s", tEntityType));
    }
  }
}
