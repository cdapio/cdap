package com.continuuity.metrics;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.metrics.service.FlowMonitorClient;
import com.continuuity.metrics.service.FlowMonitorHandler;
import com.continuuity.metrics.service.FlowMonitorServer;
import com.continuuity.metrics.stubs.Metric;
import com.continuuity.observer.StateChangeCallback;
import com.continuuity.metrics.stubs.FlowMetric;
import com.continuuity.runtime.DIModules;
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
import java.util.List;
import java.util.Random;

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
    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void after() throws Exception {
    if(zookeeper != null) {
      Closeables.closeQuietly(zookeeper);
    }
  }

  @Test
  public void testFlowMonitorServer() throws Exception {
    Injector injector = Guice.createInjector(DIModules.getInMemoryHSQLBindings());
    FlowMonitorHandler handler = injector.getInstance(FlowMonitorHandler.class);
    StateChangeCallback callback = injector.getInstance(StateChangeCallback.class);

    CConfiguration configuration = CConfiguration.create();
    configuration.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zkEnsemble);
    String dir = configuration.get("user.dir");

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

  private void generateRandomFlowMetrics(FlowMonitorClient client) throws ServiceDiscoveryClientException {
    String[] accountIds = { "demo"};
    String[] rids = { "ABC", "XYZ"};
    String[] applications = { "personalization"};
    String[] versions = { "18", "28"};
    String[] flows = { "targetting", "smart-explorer", "indexer"};
    String[] flowlets = { "flowlet-A", "flowlet-B", "flowlet-C", "flowlet-D"};
    String[] metrics = { "in", "out", "ops"};
    String[] instances = { "1", "2"};

    Random random = new Random();
    for(int i = 0; i < 100; ++i) {
      String rid = rids[(i % rids.length)];
      for(int i1 = 0; i1 < accountIds.length; ++i1) {
        int timestamp = (int)(System.currentTimeMillis()/1000);
        for(int i2 = 0; i2 < applications.length; ++i2) {
          for(int i3 = 0; i3 < versions.length; ++i3) {
            for(int i4 = 0; i4 < flows.length; ++i4) {
              for(int i5 = 0; i5 < flowlets.length; ++i5) {
                for(int i6 = 0; i6 < instances.length; ++i6) {
                  for(int i7 = 0; i7 < metrics.length; ++i7) {
                    FlowMetric metric = new FlowMetric();
                    metric.setInstance(instances[i6]);
                    metric.setFlowlet(flowlets[i5]);
                    metric.setFlow(flows[i4]);
                    metric.setVersion(versions[i3]);
                    metric.setApplication(applications[i2]);
                    metric.setAccountId(accountIds[i1]);
                    metric.setRid(rid);
                    metric.setMetric(metrics[i7]);
                    metric.setTimestamp(timestamp);
                    metric.setValue(1000 * random.nextInt() );
                    client.add(metric);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void testTestCombined() throws Exception {
    Injector injector = Guice.createInjector(DIModules.getInMemoryHSQLBindings());
    FlowMonitorHandler handler = injector.getInstance(FlowMonitorHandler.class);
    StateChangeCallback callback = injector.getInstance(StateChangeCallback.class);

    CConfiguration configuration = CConfiguration.create();
    configuration.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zkEnsemble);

    FlowMonitorServer server = new FlowMonitorServer(handler, callback);
    server.start(configuration);

    Thread.sleep(5000);

    FlowMonitorClient client = new FlowMonitorClient(configuration);
    generateRandomFlowMetrics(client);

    List<Metric> metrics = client.getFlowMetric("demo", "personalization", "targetting", "ABC");
    server.stop();
  }
}
