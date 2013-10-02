package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.zookeeper.ZKClientService;
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
    Assert.assertNotNull(RouterMain.createGuiceInjector(cConf, zkClientService));
  }
}
