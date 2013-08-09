package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.collector.FlumeCollector;
import com.continuuity.gateway.collector.NettyFlumeCollector;
import org.apache.flume.event.SimpleEvent;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the flume collector.
 */
public class FlumeCollectorTest {

  static FlumeCollector newCollector(String name) {
    FlumeCollector collector = new NettyFlumeCollector();
    collector.setName(name);
    collector.setMetadataService(new DummyMDS());
    return collector;
  }

  /**
   * verify that collector picks up the port from config.
   */
  @Test
  public void testConfiguration() throws Exception {
    String name = "notflume"; // use a different name

    CConfiguration configuration = new CConfiguration();
    FlumeCollector collector = newCollector(name);
    collector.configure(configuration);
    Assert.assertEquals(FlumeCollector.DEFAULT_PORT, collector.getPort());

    name = "otherflume";
    int port = 9000;
    configuration.setInt(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    collector = newCollector(name);
    collector.configure(configuration);
    Assert.assertEquals(port, collector.getPort());
  }

  /**
   * verify that collector does not bind to port until start().
   */
  @Test
  public void testStartStop() throws Exception {
    String name = "other";
    int port = PortDetector.findFreePort();
    String stream = "pfunk";
    // configure collector but don't start
    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    Collector collector = newCollector(name);
    collector.configure(configuration);
    collector.setConsumer(new TestUtil.NoopConsumer());
    collector.setAuthenticator(new NoAuthenticator());
    // create an event to reuse
    SimpleEvent event = TestUtil.createFlumeEvent(42, stream);
    try { // verify send fails before start()
      TestUtil.sendFlumeEvent(port, event);
      Assert.fail("Exception expected when collector has not started");
    } catch (Exception e) {
    }
    collector.start();
    // send should now succeed
    TestUtil.sendFlumeEvent(port, event);
    collector.stop();
    try { // verify send fails after stop
      TestUtil.sendFlumeEvent(port, event);
      Assert.fail("Exception expected when collector has not started");
    } catch (Exception e) {
    }
    collector.start();
    // after restart it should succeed again
    TestUtil.sendFlumeEvent(port, event);
    collector.stop();
  }

  /**
   * verify that flume events get transformed and annotated correctly.
   */
  @Test
  public void testTransformEvent() throws Exception {
    String name = "other";
    int port = PortDetector.findFreePort();
    String stream = "foo";
    int eventsToSend = 10;
    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    Collector collector = newCollector(name);
    collector.configure(configuration);
    collector.setConsumer(new TestUtil.VerifyConsumer(17, name, stream));
    collector.setAuthenticator(new NoAuthenticator());
    collector.start();
    TestUtil.sendFlumeEvent(port, TestUtil.createFlumeEvent(17, stream));
    collector.stop();
    collector.setConsumer(new TestUtil.VerifyConsumer(name, stream));
    collector.start();
    TestUtil.sendFlumeEvents(port, stream, eventsToSend, 4);
    collector.stop();
    Assert.assertEquals(eventsToSend,
        collector.getConsumer().eventsReceived());
    Assert.assertEquals(eventsToSend,
        collector.getConsumer().eventsSucceeded());
    Assert.assertEquals(0, collector.getConsumer().eventsFailed());
  }
}
