package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests connector.
 */
public class ConnectorTest {

  /**
   * This tests that a connector saves its name and configuration and
   * makes them available via getters.
   */
  @Test
  public void testConnector() throws Exception {

    Connector connector = new Connector() {
      @Override
      public void start() throws Exception {

      }

      @Override
      public void stop() throws Exception {
      }
    };

    CConfiguration config = CConfiguration.create();
    connector.setName("dummy");
    connector.configure(config);

    // test that the name is set
    Assert.assertEquals(connector.getName(), "dummy");

    // test that configuration is saved
    Assert.assertSame(connector.getConfiguration(), config);
  }


}
