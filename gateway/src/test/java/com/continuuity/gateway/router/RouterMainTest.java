package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test RouterMain.
 */
public class RouterMainTest {

  @Test
  public void testGuiceInjection() throws Exception {
    CConfiguration cConf = CConfiguration.create();

    Injector injector = RouterMain.createGuiceInjector(cConf);
    Assert.assertNotNull(injector);

    NettyRouter router = injector.getInstance(NettyRouter.class);
    Assert.assertNotNull(router);
  }
}
