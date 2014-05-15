package com.continuuity.gateway.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics.runtime.MetricsTwillRunnable;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link com.continuuity.metrics.runtime.MetricsTwillRunnable}.
 */
public class MetricsTwillRunnableTest {

  @Test
  public void testInjection() throws Exception {
    Injector injector = MetricsTwillRunnable.createGuiceInjector(CConfiguration.create(), HBaseConfiguration.create());
    Assert.assertNotNull(injector);
  }
}
