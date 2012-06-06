package com.continuuity.overlord.metrics;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.overlord.metrics.internal.FlowMetricFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FlowMetricsTest {

  @Test
  public void testSimpleFlowMetrics() throws Exception {
    Map<String, String> fields = Maps.newHashMap();
    fields.put("accountid", "accountid");
    fields.put("app", "app");
    fields.put("version", "18");
    fields.put("rid", "rid");
    fields.put("flow", "flow");
    fields.put("flowlet", "flowlet");
    fields.put("instance", "instance");

    CMetrics metrics = FlowMetricFactory.newFlowMetrics(ImmutableMap.copyOf(fields), "flow", "stream");
    FlowMonitorReporter.enable(1, TimeUnit.SECONDS, CConfiguration.create());

    for(int i = 0; i < 10000; ++i) {
      Thread.sleep(500);
      metrics.incr("in");
    }
  }
}
