package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.accessor.DataRestAccessor;
import com.continuuity.gateway.collector.FlumeCollector;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.util.Util;
import org.junit.Assert;
import org.junit.Test;

/**
 * Utility for managing configurations.
 */
public class ConfigUtilTest {

  @Test
  public void testFindConnector() {

    CConfiguration configuration = new CConfiguration();

    // set up a valid connector
    configuration.set(Constants.buildConnectorPropertyName(
        "first", Constants.CONFIG_CLASSNAME),
        NettyFlumeCollector.class.getName());

    // set up another valid connector
    configuration.set(Constants.buildConnectorPropertyName(
        "second", Constants.CONFIG_CLASSNAME),
        RestCollector.class.getName());

    // set up a connector with an invalid class name
    configuration.set(Constants.buildConnectorPropertyName(
        "third", Constants.CONFIG_CLASSNAME),
        "some.fantasy.class.name");

    // do not configure the fourth connector at all

    // 1. test the case where exactly one connector exists for the class
    configuration.set(Constants.CONFIG_CONNECTORS, "first,second");
    Assert.assertEquals("first",
        Util.findConnector(configuration, FlumeCollector.class));

    // 2. test the case where none matches
    Assert.assertNull(Util.findConnector(configuration, DataRestAccessor.class));

    // 3. test the case where more than one match
    Assert.assertNull(Util.findConnector(configuration, Collector.class));

    // 4. add some invalid connectors and verify that this still works
    configuration.set(Constants.CONFIG_CONNECTORS, "first,second,third,fourth");
    Assert.assertEquals("first",
        Util.findConnector(configuration, FlumeCollector.class));
    Assert.assertNull(Util.findConnector(configuration, DataRestAccessor.class));
    Assert.assertNull(Util.findConnector(configuration, Collector.class));
  }

}
