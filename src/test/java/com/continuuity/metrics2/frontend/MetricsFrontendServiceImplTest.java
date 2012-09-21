package com.continuuity.metrics2.frontend;

import akka.dispatch.Await;
import akka.util.Duration;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.metrics2.collector.MetricResponse;
import com.continuuity.metrics2.collector.server.plugins.FlowMetricsProcessor;
import com.continuuity.metrics2.collector.server.plugins.MetricsProcessor;
import com.continuuity.metrics2.stubs.*;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests the metrics service.
 */
public class MetricsFrontendServiceImplTest {
  private static String connectionUrl;
  private static CConfiguration configuration;
  private static MetricsProcessor processor = null;
  private static MetricsFrontendService.Iface client = null;


  @BeforeClass
  public static void beforeClass() throws Exception  {
    connectionUrl = "jdbc:hsqldb:mem:metrictest?user=sa";
    configuration = CConfiguration.create();
    configuration.set(Constants.CFG_METRICS_CONNECTION_URL, connectionUrl);
    processor = new FlowMetricsProcessor(configuration);
    Assert.assertNotNull(processor);
    client = new MetricsFrontendServiceImpl(configuration);
    Assert.assertNotNull(client);
  }

  @Test(expected = MetricsServiceException.class)
  public void testBadRequestArguments() throws Exception {
    getMetric(new FlowArgument(), null);
  }

  /**
   * Tests writing and reading a metric.
   */
  @Test(timeout = 2000)
  public void testAddingSingleMetricAndReadingItBack() throws Exception {
    Assert.assertTrue(
      addMetric("accountId.applicationId.flowId.runId.flowletId.1.processed",
                10) == MetricResponse.Status.SUCCESS);
    List<Counter> counters = getMetric(
      new FlowArgument("accountId", "applicationId", "flowId"),
      null
    );
    Assert.assertNotNull(counters);
    Assert.assertThat(counters.size(), CoreMatchers.is(1));
    Assert.assertTrue(counters.get(0).getValue() == 10.0f);
  }

  @Test(timeout = 2000)
  public void testAddingMultipleFlowletsForSingleMetric() throws Exception {
    addMetric("demo.myapp.myflow.myfun.source.1.processed", 10);
    addMetric("demo.myapp.myflow.myfun.compute.1.processed", 11);
    addMetric("demo.myapp.myflow.myfun.sink.1.processed", 12);
    List<Counter> counters = getMetric(
      new FlowArgument("demo", "myapp", "myflow"),
      null
    );
    Assert.assertNotNull(counters);
    Assert.assertThat(counters.size(), CoreMatchers.is(3));
  }

  @Test
  public void testTimeseriesBasic() throws Exception {

    for(int i = 0; i < 10; ++i) {
      addMetric("demo.myapp.myflow.myfun.source.1.processed", 10);
      addMetric("demo.myapp.myflow.myfun.compute.1.processed", 11);
      addMetric("demo.myapp.myflow.myfun.sink.1.processed", 12);
      Thread.sleep(1000);
    }

    List<String> metrics = Lists.newArrayList();
    metrics.add("processed");
    long ets = System.currentTimeMillis()/1000;
    long sts = ets - 30;
    DataPoints pointsFlowLevel = getTimeseries(
      new FlowArgument("demo", "myapp", "myflow"),
      metrics,
      MetricTimeseriesLevel.FLOW_LEVEL,
      sts,
      ets
    );
    Assert.assertTrue(pointsFlowLevel.getPoints().get("processed").size() > 1);

    DataPoints pointsAppLevel = getTimeseries(
      new FlowArgument("demo", "myapp", "myflow"),
      metrics,
      MetricTimeseriesLevel.APPLICATION_LEVEL,
      sts,
      ets
    );
    Assert.assertTrue(pointsAppLevel.equals(pointsFlowLevel));

    DataPoints accountLevel = getTimeseries(
      new FlowArgument("demo", "myapp", "myflow"),
      metrics,
      MetricTimeseriesLevel.ACCOUNT_LEVEL,
      sts,
      ets
    );
    Assert.assertTrue(accountLevel.equals(pointsFlowLevel));
    Assert.assertTrue(accountLevel.equals(pointsAppLevel));

    FlowArgument arg = new FlowArgument("demo", "myapp", "myflow");
    arg.setFlowletId("source");
    DataPoints pointsFlowletLevel = getTimeseries(
      arg,
      metrics,
      MetricTimeseriesLevel.FLOWLET_LEVEL,
      sts,
      ets
    );
    Assert.assertTrue(pointsFlowletLevel.getPoints().get("processed").size() >
                        1);

  }


  @Test(timeout = 2000)
  public void testMultipleFlowletsAndMultipleInstancePerFlowlet() throws
    Exception {
    // Three sources
    addMetric("demo.myapp.myflow.myfun.source.1.processed", 10);
    addMetric("demo.myapp.myflow.myfun.source.2.processed", 10);
    addMetric("demo.myapp.myflow.myfun.source.3.processed", 10);

    // Four computes
    addMetric("demo.myapp.myflow.myfun.compute.1.processed", 11);
    addMetric("demo.myapp.myflow.myfun.compute.2.processed", 11);
    addMetric("demo.myapp.myflow.myfun.compute.3.processed", 11);
    addMetric("demo.myapp.myflow.myfun.compute.4.processed", 11);

    // Three sinks
    addMetric("demo.myapp.myflow.myfun.sink.1.processed", 12);
    addMetric("demo.myapp.myflow.myfun.sink.2.processed", 12);
    addMetric("demo.myapp.myflow.myfun.sink.3.processed", 12);

    // Expectation is that all instance counts are aggregated into
    // the flowlet.
    List<Counter> counters = getMetric(
      new FlowArgument("demo", "myapp", "myflow"),
      null
    );
    Assert.assertNotNull(counters);
    Assert.assertThat(counters.size(), CoreMatchers.is(3));
  }

  /**
   * @return Status of adding a metric.
   */
  private MetricResponse.Status addMetric(String name, float value)
      throws Exception {

    com.continuuity.metrics2.collector.MetricRequest
      request = new com.continuuity.metrics2.collector.MetricRequest.Builder(true)
      .setRequestType("put")
      .setMetricName(name)
      .setTimestamp(System.currentTimeMillis()/1000)
      .setValue(value)
      .setMetricType("FlowSystem")
      .create();

    // Blocks till we get the response back.
    return Await.result(processor.process(request),
                        Duration.create(2, TimeUnit.SECONDS));
  }

  private List<Counter> getMetric(FlowArgument argument, List<String> names)
    throws TException, MetricsServiceException {
    CounterRequest request = new CounterRequest(argument);
    if(names != null) {
      request.setName(names);
    }
    return client.getCounters(request);
  }

  private DataPoints getTimeseries(FlowArgument argument,
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

}
