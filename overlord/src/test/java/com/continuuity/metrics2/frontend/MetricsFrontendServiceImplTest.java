package com.continuuity.metrics2.frontend;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricRequest;
import com.continuuity.common.metrics.MetricResponse;
import com.continuuity.metrics2.collector.plugins.FlowMetricsProcessor;
import com.continuuity.metrics2.collector.plugins.MetricsProcessor;
import com.continuuity.metrics2.temporaldb.DataPoint;
import com.continuuity.metrics2.thrift.Counter;
import com.continuuity.metrics2.thrift.CounterRequest;
import com.continuuity.metrics2.thrift.FlowArgument;
import com.continuuity.metrics2.thrift.MetricTimeseriesLevel;
import com.continuuity.metrics2.thrift.MetricsFrontendService;
import com.continuuity.metrics2.thrift.MetricsServiceException;
import com.continuuity.metrics2.thrift.Points;
import com.continuuity.metrics2.thrift.TimeseriesRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Tests the metrics service.
 */
public class MetricsFrontendServiceImplTest {
  private static String connectionUrl;
  private static CConfiguration configuration;
  private static MetricsProcessor processor = null;
  private static MetricsFrontendService.Iface client = null;
  private static long timestamp = System.currentTimeMillis() / 1000;
  private static long counter = 0;
  private static int POINTS = 10;


  @BeforeClass
  public static void beforeClass() throws Exception  {
    connectionUrl = "jdbc:hsqldb:mem:metrictest?user=sa";
    configuration = CConfiguration.create();
    configuration.set(Constants.CFG_METRICS_CONNECTION_URL, connectionUrl);
    configuration.set(Constants.CFG_METRICS_COLLECTION_ALLOWED_TIMESERIES_METRICS,
                      "processed,acks,tuples.read.count,tuples.proc.count");
    processor = new FlowMetricsProcessor(configuration);
    Assert.assertNotNull(processor);
    client = new MetricsFrontendServiceImpl(configuration);
    Assert.assertNotNull(client);
    createDataSet();
  }

  private static void createDataSet() throws Exception {
    Random random = new Random();
    counter = 0; // Initialize time.

    // Add multiple flows, on multiple accounts, with multiple run ids
    // multiple flowlets, with multiple applications and multiple metrics
    for (int i = 0; i < POINTS; ++i)  {
      addMetric("acc0.app1.flow1.runid1.fl1.1.processed", 10);
      addMetric("acc0.app1.flow1.runid1.fl2.1.processed", 11 * i);
      addMetric("acc0.app1.flow1.runid1.fl3.1.processed", 12 * (i + 1));
      addMetric("acc0.app1.flow1.runid1.fl4.1.processed", 13 * (i + 2));

      // Another metric
      addMetric("acc0.app1.flow1.runid1.fl1.1.acks", 1);
      addMetric("acc0.app1.flow1.runid1.fl2.1.acks", 2 * i);
      addMetric("acc0.app1.flow1.runid1.fl3.1.acks", 3 * (i + 1));
      addMetric("acc0.app1.flow1.runid1.fl4.1.acks", 4 * (i + 2));

      addMetric("acc0.app1.flow1.runid1.fl1.1.tuples.read.count", 10);
      addMetric("acc0.app1.flow1.runid1.fl2.1.tuples.read.count", 11 * i);
      addMetric("acc0.app1.flow1.runid1.fl3.1.tuples.read.count", 12 * (i + 1));
      addMetric("acc0.app1.flow1.runid1.fl4.1.tuples.read.count", 13 * (i + 2));

      addMetric("acc0.app1.flow1.runid1.fl1.1.tuples.proc.count", 10);
      addMetric("acc0.app1.flow1.runid1.fl2.1.tuples.proc.count", 8 * i);
      addMetric("acc0.app1.flow1.runid1.fl3.1.tuples.proc.count", 14 * (i + 1));
      addMetric("acc0.app1.flow1.runid1.fl4.1.tuples.proc.count", 7 * (i + 2));

      // Single flow, multiple flowlets
      addMetric("acc1.app1.flow1.runid1.fl1.1.processed", random.nextInt());
      addMetric("acc1.app1.flow1.runid1.fl2.1.processed", random.nextInt());
      addMetric("acc1.app1.flow1.runid1.fl3.1.processed", random.nextInt());
      addMetric("acc1.app1.flow1.runid1.fl4.1.processed", random.nextInt());

      // Another app within the same account.
      addMetric("acc1.app2.flow1.runid1.fl11.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid1.fl12.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid1.fl13.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid1.fl14.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid1.fl15.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid1.fl16.1.processed", random.nextInt());

      // Another flow within app2
      addMetric("acc1.app2.flow2.runid1.fl11.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid1.fl12.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid1.fl13.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid1.fl14.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid1.fl15.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid1.fl16.1.processed", random.nextInt());

      addMetric("acc1.app2.flow3.runid1.fl11.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid1.fl12.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid1.fl13.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid1.fl14.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid1.fl15.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid1.fl16.1.processed", random.nextInt());

      // Multiple run ids for all flows
      addMetric("acc1.app2.flow1.runid2.fl11.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid2.fl12.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid2.fl13.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid2.fl14.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid2.fl15.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid2.fl16.1.processed", random.nextInt());

      addMetric("acc1.app2.flow1.runid3.fl11.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid3.fl12.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid3.fl13.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid3.fl14.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid3.fl15.1.processed", random.nextInt());
      addMetric("acc1.app2.flow1.runid3.fl16.1.processed", random.nextInt());

      addMetric("acc1.app2.flow2.runid2.fl11.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid2.fl12.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid2.fl13.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid2.fl14.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid2.fl15.1.processed", random.nextInt());
      addMetric("acc1.app2.flow2.runid2.fl16.1.processed", random.nextInt());

      addMetric("acc1.app2.flow3.runid2.fl11.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid2.fl12.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid2.fl13.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid2.fl14.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid2.fl15.1.processed", random.nextInt());
      addMetric("acc1.app2.flow3.runid2.fl16.1.processed", random.nextInt());

      // Flow on different account
      addMetric("acc2.app1.flow1.runid1.fl11.1.processed", random.nextInt());
      addMetric("acc2.app1.flow1.runid1.fl12.1.processed", random.nextInt());
      addMetric("acc2.app1.flow1.runid1.fl13.1.processed", random.nextInt());
      addMetric("acc2.app1.flow1.runid1.fl14.1.processed", random.nextInt());
      addMetric("acc2.app1.flow1.runid1.fl15.1.processed", random.nextInt());
      addMetric("acc2.app1.flow1.runid1.fl16.1.processed", random.nextInt());

      // Another flow
      addMetric("acc2.app1.flow2.runid1.fl11.1.processed", random.nextInt());
      addMetric("acc2.app1.flow2.runid1.fl12.1.processed", random.nextInt());
      addMetric("acc2.app1.flow2.runid1.fl13.1.processed", random.nextInt());
      addMetric("acc2.app1.flow2.runid1.fl14.1.processed", random.nextInt());
      addMetric("acc2.app1.flow2.runid1.fl15.1.processed", random.nextInt());
      addMetric("acc2.app1.flow2.runid1.fl16.1.processed", random.nextInt());

      // Another account
      addMetric("acc3.app1.flow1.runid1.fl11.1.processed", random.nextInt());
      addMetric("acc3.app1.flow1.runid1.fl12.1.processed", random.nextInt());
      addMetric("acc3.app1.flow1.runid1.fl13.1.processed", random.nextInt());
      addMetric("acc3.app1.flow1.runid1.fl14.1.processed", random.nextInt());
      addMetric("acc3.app1.flow1.runid1.fl15.1.processed", random.nextInt());
      addMetric("acc3.app1.flow1.runid1.fl16.1.processed", random.nextInt());

      // Another flow
      addMetric("acc3.app1.flow2.runid1.fl11.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid1.fl12.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid1.fl13.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid1.fl14.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid1.fl15.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid1.fl16.1.processed", random.nextInt());

      // multiple run id on
      addMetric("acc3.app1.flow2.runid2.fl11.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid2.fl12.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid2.fl13.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid2.fl14.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid2.fl15.1.processed", random.nextInt());
      addMetric("acc3.app1.flow2.runid2.fl16.1.processed", random.nextInt());
      counter++; // move the time ahead.
    }
  }

  @Test(expected = MetricsServiceException.class)
  public void testBadRequestArguments() throws Exception {
    getCounters(new FlowArgument(), null);
  }

  @Test
  public void testOneAcctOneFlowOneRunId() throws Exception {
    List<String> metrics = Lists.newArrayList();
    metrics.add("processed");
    FlowArgument argument = new FlowArgument("acc0", "app1", "flow1");
    argument.setRunId("runid1");
    argument.setFlowletId("fl1");

    Points dataPointsFlowLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.FLOW_LEVEL,
      timestamp,
      timestamp + counter
    );

    Assert.assertNotNull(dataPointsFlowLevel);
    Assert.assertTrue(dataPointsFlowLevel.getPoints()
                        .get("processed").size() >= 1);

    Points dataPointsRunIdLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.RUNID_LEVEL,
      timestamp,
      timestamp + counter
    );
    Assert.assertNotNull(dataPointsRunIdLevel);
    Assert.assertTrue(dataPointsRunIdLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsFlowLevel.equals(dataPointsRunIdLevel));
  }

  @Test
  public void testOneAcctOneFlowOneRunIdMultipleMetrics() throws Exception {
    List<String> metrics = Lists.newArrayList();
    metrics.add("processed");
    metrics.add("acks");

    FlowArgument argument = new FlowArgument("acc0", "app1", "flow1");
    argument.setRunId("runid1");
    argument.setFlowletId("fl1");

    Points dataPointsFlowLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.FLOW_LEVEL,
      timestamp,
      timestamp + counter
    );

    Assert.assertNotNull(dataPointsFlowLevel);
    Assert.assertTrue(dataPointsFlowLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsFlowLevel.getPoints()
                        .get("acks").size() >= 1);

    Points dataPointsRunIdLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.RUNID_LEVEL,
      timestamp,
      timestamp + counter
    );
    Assert.assertNotNull(dataPointsRunIdLevel);
    Assert.assertTrue(dataPointsRunIdLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsRunIdLevel.getPoints()
                        .get("acks").size() >= 1);
    Assert.assertTrue(dataPointsFlowLevel.equals(dataPointsRunIdLevel));
  }

  @Test
  public void testWhatHappensWhenMetricNotFound() throws Exception {
    List<String> metrics = Lists.newArrayList();
    metrics.add("p1");

    FlowArgument argument = new FlowArgument("acc0", "app1", "flow1");
    argument.setRunId("runid1");
    argument.setFlowletId("fl1");

    Points dataPointsFlowLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.FLOW_LEVEL,
      timestamp,
      timestamp + counter
    );

    Assert.assertNotNull(dataPointsFlowLevel);
  }

  /**
   * For a single account the values at different levels should be the same.
   * Account Level, Application Level, Flow Level, Run Id Level.
   */
  @Test
  public void testOneAcctOneFlowOneRunIdMultipleMetricsAppAccount()
    throws Exception {
    List<String> metrics = Lists.newArrayList();
    metrics.add("processed");
    metrics.add("acks");

    FlowArgument argument = new FlowArgument("acc0", "app1", "flow1");
    argument.setRunId("runid1");
    argument.setFlowletId("fl1");

    Points dataPointsFlowLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.FLOW_LEVEL,
      timestamp,
      timestamp + counter
    );

    Assert.assertNotNull(dataPointsFlowLevel);
    Assert.assertTrue(dataPointsFlowLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsFlowLevel.getPoints()
                        .get("acks").size() >= 1);

    Points dataPointsRunIdLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.RUNID_LEVEL,
      timestamp,
      timestamp + counter
    );
    Assert.assertNotNull(dataPointsRunIdLevel);
    Assert.assertTrue(dataPointsRunIdLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsRunIdLevel.getPoints()
                        .get("acks").size() >= 1);
    Assert.assertTrue(dataPointsFlowLevel.equals(dataPointsRunIdLevel));

    Points dataPointsAppLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.APPLICATION_LEVEL,
      timestamp,
      timestamp + counter
    );
    Assert.assertNotNull(dataPointsAppLevel);
    Assert.assertTrue(dataPointsAppLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsAppLevel.getPoints()
                        .get("acks").size() >= 1);
    Assert.assertTrue(dataPointsAppLevel.equals(dataPointsRunIdLevel));
    Assert.assertTrue(dataPointsAppLevel.equals(dataPointsFlowLevel));

    Points dataPointsAccountLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.ACCOUNT_LEVEL,
      timestamp,
      timestamp + counter
    );
    Assert.assertNotNull(dataPointsAppLevel);
    Assert.assertTrue(dataPointsAccountLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsAccountLevel.getPoints()
                        .get("acks").size() >= 1);
    Assert.assertTrue(dataPointsAccountLevel.equals(dataPointsRunIdLevel));
    Assert.assertTrue(dataPointsAccountLevel.equals(dataPointsFlowLevel));
    Assert.assertTrue(dataPointsAccountLevel.equals(dataPointsAppLevel));

    Points dataPointsFlowletLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.FLOWLET_LEVEL,
      timestamp,
      timestamp + counter
    );
    Assert.assertNotNull(dataPointsFlowletLevel);
    Assert.assertTrue(dataPointsFlowletLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsFlowletLevel.getPoints()
                        .get("acks").size() >= 1);
  }

  @Test
  public void testWithMultipleAccounts() throws Exception {
    List<String> metrics = Lists.newArrayList();
    metrics.add("processed");
    metrics.add("acks");

    FlowArgument argument = new FlowArgument("acc0", "app1", "flow1");
    argument.setRunId("runid1");
    argument.setFlowletId("fl1");

    Points dataPointsFlowLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.FLOW_LEVEL,
      timestamp,
      timestamp + counter
    );

    Assert.assertNotNull(dataPointsFlowLevel);
    Assert.assertTrue(dataPointsFlowLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsFlowLevel.getPoints()
                        .get("acks").size() >= 1);

    Points dataPointsRunIdLevel = getTimeseries(
      argument,
      metrics,
      MetricTimeseriesLevel.RUNID_LEVEL,
      timestamp,
      timestamp + counter
    );
    Assert.assertNotNull(dataPointsRunIdLevel);
    Assert.assertTrue(dataPointsRunIdLevel.getPoints()
                        .get("processed").size() >= 1);
    Assert.assertTrue(dataPointsRunIdLevel.getPoints()
                        .get("acks").size() >= 1);
    Assert.assertTrue(dataPointsFlowLevel.equals(dataPointsRunIdLevel));
  }

  /**
   * @return Status of adding a metric.
   */
  private static MetricResponse.Status addMetric(String name, float value)
      throws Exception {

    com.continuuity.common.metrics.MetricRequest
      request =
      new MetricRequest.Builder(true)
      .setRequestType("put")
      .setMetricName(name)
      .setTimestamp(timestamp + counter)
      .setValue(value)
      .setMetricType("FlowSystem")
      .create();

    // Blocks till we get the response back.
    return processor.process(request).get(2, TimeUnit.SECONDS);
  }

  private List<Counter> getCounters(FlowArgument argument, List<String> names)
    throws TException, MetricsServiceException {
    CounterRequest request = new CounterRequest(argument);
    if (names != null) {
      request.setName(names);
    }
    return client.getCounters(request);
  }

  private Points getTimeseries(FlowArgument argument,
                                   List<String> metrics,
                                   MetricTimeseriesLevel level,
                                   long start,
                                   long end)
    throws TException, MetricsServiceException {
    TimeseriesRequest request = new TimeseriesRequest();
    request.setArgument(argument);
    request.setMetrics(metrics);
    request.setStartts(start);
    request.setLevel(level);
    request.setEndts(end);
    request.setSummary(true);
    return client.getTimeSeries(request);
  }

  @Test(expected = NullPointerException.class)
  public void testNullImmutableCopyList() throws Exception {
    List<DataPoint> p = new ArrayList<DataPoint>();
    ImmutableList<DataPoint> r = ImmutableList.copyOf(p);
    Assert.assertNotNull(r);
    p = null;
    r = ImmutableList.copyOf(p);
  }

}
