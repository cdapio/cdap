package com.continuuity.overlord.metrics.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.metrics.service.FlowMonitorClient;
import com.continuuity.metrics.service.FlowMonitorHandler;
import com.continuuity.metrics.service.FlowMonitorServer;
import com.continuuity.flowmanager.StateChangeCallback;
import com.continuuity.metrics.CMetrics;
import com.continuuity.metrics.FlowMonitorReporter;
import com.continuuity.metrics.internal.FlowMetricFactory;
import com.continuuity.metrics.stubs.FlowMetric;
import com.continuuity.runtime.DIModules;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 *
 */
public class FlowMonitorServerTest {
  private static final Logger Log = LoggerFactory.getLogger(FlowMonitorServerTest.class);
  private static InMemoryZookeeper zookeeper = null;
  private static String zkEnsemble;

  @BeforeClass
  public static void before() throws Exception {
    zookeeper = new InMemoryZookeeper();
    zkEnsemble = zookeeper.getConnectionString();
    Log.info("Connection string {}", zkEnsemble);
    org.hsqldb.util.DatabaseManagerSwing.main(new String[] {
      "--url",  "jdbc:hsqldb:mem:testdb", "--noexit"
    });
  }

  @AfterClass
  public static void after() throws Exception {
    if(zookeeper != null) {
      Closeables.closeQuietly(zookeeper);
    }
  }

  @Test
  public void testFlowMonitorServer() throws Exception {
    Injector injector = Guice.createInjector(DIModules.getInMemoryHSQLModules());
    FlowMonitorHandler handler = injector.getInstance(FlowMonitorHandler.class);
    StateChangeCallback callback = injector.getInstance(StateChangeCallback.class);

    CConfiguration configuration = CConfiguration.create();
    configuration.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zkEnsemble);

    FlowMonitorServer server = new FlowMonitorServer(handler, callback);
    server.start(configuration);

    Thread.sleep(5000);

    FlowMonitorClient client = new FlowMonitorClient(configuration);

    FlowMetric metric = new FlowMetric();
    metric.setAccountId("accountid");
    metric.setApplication("application");
    metric.setRid("rid");
    metric.setFlow("flow");
    metric.setFlowlet("flowlet");
    metric.setVersion("version");
    metric.setInstance("instance");
    metric.setMetric("metric");
    metric.setValue(10);

    client.add(metric);

    Thread.sleep(1000);

    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:fmdb", "sa", "");
    String sql = "SELECT COUNT(*)  FROM flow_metrics;";
    PreparedStatement stmt = connection.prepareStatement(sql);
    ResultSet rs = stmt.executeQuery();
    Assert.assertTrue(rs.getFetchSize() == 1);
    rs.next();
    int i = rs.getInt(1);

    sql = "SELECT * FROM flow_metrics";
    stmt = connection.prepareStatement(sql);
    rs = stmt.executeQuery();
    rs.next();
    String flowlet = rs.getString("flowlet");
    Assert.assertTrue("flowlet".equals(flowlet));

    server.stop();
  }

  @Test
  public void testTestCombined() throws Exception {

    Map<String, String> fields = Maps.newHashMap();
    fields.put("accountid", "demo");
    fields.put("app", "personalization");
    fields.put("version", "18");
    fields.put("rid", "rid1");
    fields.put("flow", "simple");
    fields.put("flowlet", "sink");
    fields.put("instance", "1");

    CMetrics sinkMetrics = FlowMetricFactory.newFlowMetrics(ImmutableMap.copyOf(fields), "flow", "stream");

    fields.put("flowlet", "source");
    CMetrics sourceMetrics = FlowMetricFactory.newFlowMetrics(ImmutableMap.copyOf(fields), "flow", "stream");

    Injector injector = Guice.createInjector(DIModules.getInMemoryHSQLModules());
    FlowMonitorHandler handler = injector.getInstance(FlowMonitorHandler.class);
    StateChangeCallback callback = injector.getInstance(StateChangeCallback.class);

    CConfiguration configuration = CConfiguration.create();
    configuration.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zkEnsemble);

    FlowMonitorServer server = new FlowMonitorServer(handler, callback);
    server.start(configuration);

    Thread.sleep(5000);

    sinkMetrics.incr("in");
    sinkMetrics.incr("in1");
    sourceMetrics.incr("out");
    sourceMetrics.incr("out1");

    FlowMonitorReporter.enable(1, TimeUnit.SECONDS, configuration);

    for(int i = 0; i< 1000; ++i) {
      Thread.sleep(10);
      sinkMetrics.incr("in");
      sinkMetrics.incr("in1");
      sourceMetrics.incr("out");
      sourceMetrics.incr("out1");
    }

    Thread.sleep(2000);
    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:fmdb", "sa", "");
    String sql = "SELECT DISTINCT flowlet FROM flow_metrics";
    PreparedStatement stmt = connection.prepareStatement(sql);
    ResultSet rs = stmt.executeQuery();
    while(rs.next()) {
      String name1 = rs.getString(1);
      Assert.assertTrue("sink".equals(name1) || "source".equals(name1));
    }
    server.stop();
  }
}
