package com.continuuity.metrics2.api;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.collector.MetricType;
import com.continuuity.metrics2.collector.OverlordMetricsReporter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Testing of flow metric API.
 */
public class CMetricsTest {

//  @BeforeClass
//  public static void beforeTest() throws Exception {
//    OverlordMetricsReporter.enable(1, TimeUnit.SECONDS, CConfiguration.create());
//  }
//
//  @Test
//  public void simpleFlowMetricTest() throws Exception {
//    CMetrics systemMetrics
//      = new CMetrics(MetricType.FlowSystem,
//            "act.app.flow.run.let.1");
//
//    CMetrics userMetrics
//      = new CMetrics(MetricType.FlowUser,
//                     "act.app.flow.run.let.2");
//
//    for(int i = 0; i < 1000; i++) {
//      systemMetrics.meter(CMetricsTest.class, "stream.in", 1);
//      systemMetrics.meter("stream.out", 1);
//      systemMetrics.histogram("window", 1);
//      userMetrics.histogram("mytest", 1);
//      Thread.sleep(1000);
//    }
//
//  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyMetricGroupForFlowSystem() throws Exception {
    CMetrics metrics = new CMetrics(MetricType.FlowSystem);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyMetricGroupForFlowUser() throws Exception {
    CMetrics metrics = new CMetrics(MetricType.FlowUser);
  }
}
