package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.zookeeper.ZKClientService;
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
    String zookeeper = cConf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE);

    ZKClientService zkClientService = ZKClientService.Builder.of(zookeeper).build();
    Injector injector = RouterMain.createGuiceInjector(cConf, zkClientService);
    Assert.assertNotNull(injector);

    NettyRouter router = injector.getInstance(NettyRouter.class);
    Assert.assertNotNull(router);
  }
}
