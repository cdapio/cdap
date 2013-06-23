package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.util.HttpConfig;
import org.junit.Assert;
import org.junit.Test;

public class HttpConfigTest {

  /**
   * verify that config picks up the port, path and other options from config
   */
  @Test
  public void testConfiguration() throws Exception {
    String name = "restful"; // use a different name

    // test default configuration
    HttpConfig defaults = new HttpConfig(name);
    Assert.assertEquals(name, defaults.getName());
    Assert.assertEquals(HttpConfig.DEFAULT_MIDDLE, defaults.getPathMiddle());
    Assert.assertEquals(HttpConfig.DEFAULT_PREFIX, defaults.getPathPrefix());
    Assert.assertEquals(HttpConfig.DEFAULT_PORT, defaults.getPort());
    Assert.assertEquals(HttpConfig.DEFAULT_SSL, defaults.isSsl());
    Assert.assertEquals(HttpConfig.DEFAULT_CHUNKING, defaults.isChunking());
    Assert.assertEquals(HttpConfig.DEFAULT_MAX_CONTENT_SIZE,
        defaults.getMaxContentSize());

    // test that setters work
    int port = 1234;
    String prefix = "/continewity";
    String middle = "/pathway/";
    defaults.setPort(port)
        .setPathPrefix(prefix)
        .setPathMiddle(middle);
    Assert.assertEquals(name, defaults.getName());
    Assert.assertEquals(middle, defaults.getPathMiddle());
    Assert.assertEquals(prefix, defaults.getPathPrefix());
    Assert.assertEquals(port, defaults.getPort());
    Assert.assertEquals(HttpConfig.DEFAULT_SSL, defaults.isSsl());
    Assert.assertEquals(HttpConfig.DEFAULT_CHUNKING, defaults.isChunking());
    Assert.assertEquals(HttpConfig.DEFAULT_MAX_CONTENT_SIZE,
        defaults.getMaxContentSize());

    // test that defaults carry over
    name = "rusty";
    CConfiguration configuration = new CConfiguration();
    HttpConfig config = HttpConfig.configure(name, configuration, defaults);
    Assert.assertEquals(name, config.getName());
    Assert.assertEquals(middle, config.getPathMiddle());
    Assert.assertEquals(prefix, config.getPathPrefix());
    Assert.assertEquals(port, config.getPort());
    Assert.assertEquals(HttpConfig.DEFAULT_SSL, config.isSsl());
    Assert.assertEquals(HttpConfig.DEFAULT_CHUNKING, config.isChunking());
    Assert.assertEquals(HttpConfig.DEFAULT_MAX_CONTENT_SIZE,
        config.getMaxContentSize());

    // test that configure() picks up all parameters
    name = "resty";
    port = 5555;
    prefix = "/continuuity";
    middle = "/destination/";
    int maxSize = 2 * HttpConfig.DEFAULT_MAX_CONTENT_SIZE;
    configuration = new CConfiguration();
    configuration.setInt(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    configuration.set(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), middle);
    configuration.setBoolean(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_CHUNKING),
        !HttpConfig.DEFAULT_CHUNKING);
    configuration.setBoolean(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_SSL), true);
    configuration.setInt(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_MAX_SIZE), maxSize);
    config = HttpConfig.configure(name, configuration, defaults);
    Assert.assertEquals(port, config.getPort());
    Assert.assertEquals(prefix, config.getPathPrefix());
    Assert.assertEquals(middle, config.getPathMiddle());
    Assert.assertEquals(!HttpConfig.DEFAULT_CHUNKING, config.isChunking());
    Assert.assertEquals(maxSize, config.getMaxContentSize());
    Assert.assertFalse(config.isSsl());
  }

}
