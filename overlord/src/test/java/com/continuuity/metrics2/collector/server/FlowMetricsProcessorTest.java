package com.continuuity.metrics2.collector.server;

import akka.dispatch.Await;
import akka.util.Duration;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricRequest;
import com.continuuity.common.metrics.MetricResponse;
import com.continuuity.metrics2.collector.plugins.FlowMetricsProcessor;
import com.continuuity.metrics2.collector.plugins.MetricsProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests Flow Metric Processor.
 */
public class FlowMetricsProcessorTest {
  private static String connectionUrl;
  private static CConfiguration configuration;

  // Static section executed when class is created.
  {
    connectionUrl = "jdbc:hsqldb:mem:metrictest?user=sa";
    configuration = CConfiguration.create();
    configuration.setStrings(Constants.CFG_METRICS_CONNECTION_URL,
                             connectionUrl);
  }

  @Test
  public void testAddingMetric() throws Exception {
    MetricsProcessor processor
      = new FlowMetricsProcessor(configuration);

    long timestamp = System.currentTimeMillis()/1000;
    for(int i = 1; i <= 10; ++i) {
      MetricRequest request = new MetricRequest.Builder(true)
        .setRequestType("put")
        .setMetricName("accountId.applicationId.flowId.runId.flowletId.1.processed")
        .setTimestamp(timestamp+i) // move the time to avoid sleeping for test.
        .setValue(i)
        .setMetricType("FlowSystem")
        .create();
        // Makes a blocking call.
        MetricResponse.Status status = processor.process(request).get(1, TimeUnit.SECONDS);
        Assert.assertTrue(status == MetricResponse.Status.SUCCESS);
    }

    // Now try reading the metric add back from sql.
    Connection connection = DriverManager.getConnection(connectionUrl);

    assertNotNull(connection);
    String readSQL = "SELECT * FROM metrics";
    Statement stmt = null;
    try {
       stmt = connection.createStatement();
       assertNotNull(stmt);
       if(stmt.execute(readSQL)) {
         ResultSet rs = stmt.getResultSet();
         assertNotNull(rs);
         // We expect only one row here.
         while(rs.next()) {
           assertTrue(10.0 == rs.getFloat("value"));
           assertThat(rs.getInt("instance_id"), is(1));
           assertThat(rs.getString("account_id"), equalTo("accountId"));
           assertThat(rs.getString("application_id"), equalTo("applicationId"));
           assertThat(rs.getString("flow_id"), equalTo("flowId"));
           assertThat(rs.getString("flowlet_id"), equalTo("flowletId"));
         }
       }
    } finally {
      if(stmt != null) {
        stmt.close();
      }
    }
  }


}
