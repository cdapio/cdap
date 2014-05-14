package com.continuuity.gateway.run;

import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Injector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test {@link GatewayTwillRunnable}.
 */
public class GatewayTwillRunnableTest {

  @Test
  public void testInjection() throws Exception {
    Injector injector = GatewayTwillRunnable.createGuiceInjector(CConfiguration.create(), HBaseConfiguration.create());
    Assert.assertNotNull(injector);
  }
}
