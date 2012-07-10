package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.collector.RestCollector;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Test;

public class RestCollectorTest {

  static RestCollector newCollector(String name) {
    RestCollector collector = new RestCollector();
    collector.setName(name);
    return collector;
  }

  /**
   * verify that collector does not bind to port until start()
   */
  @Test
  public void testStartStop() throws Exception {
    String name = "rusty";
    String prefix = "/continuuity";
    String path = "/q/";
    String destination = "pfunk";
    int port = TestUtil.findFreePort();
    // configure collector but don't start
    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    configuration.set(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), path);
    Collector collector = newCollector(name);
    collector.configure(configuration);
    collector.setConsumer(new TestUtil.NoopConsumer());
    // create an http post
    HttpPost post =
        TestUtil.createHttpPost(port, prefix, path, destination, 42);
    try { // verify send fails before start()
      TestUtil.sendRestEvent(post);
      Assert.fail("Exception expected when collector has not started");
    } catch (Exception e) {
    }
    collector.start();
    // send should now succeed
    TestUtil.sendRestEvent(post);
    collector.stop();
    try { // verify send fails after stop
      TestUtil.sendRestEvent(post);
      Assert.fail("Exception expected when collector has not started");
    } catch (Exception e) {
    }
    collector.start();
    // after restart it should succeed again
    TestUtil.sendRestEvent(post);
    collector.stop();
  }

  /**
   * verify that rest events get transformed and annotated correctly
   */
  @Test
  public void testTransformEvent() throws Exception {
    String name = "other";
    String prefix = "/data";
    String path = "/stream/";
    String destination = "foo/bar";
    int eventsToSend = 10;
    int port = TestUtil.findFreePort();
    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    configuration.set(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.
        buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), path);
    Collector collector = newCollector(name);
    collector.configure(configuration);
    collector.setConsumer(new TestUtil.VerifyConsumer(15, name, destination));
    collector.start();
    TestUtil.sendRestEvent(TestUtil.
        createHttpPost(port, prefix, path, destination, 15));
    collector.stop();
    collector.setConsumer(new TestUtil.VerifyConsumer(name, destination));
    collector.start();
    TestUtil.sendRestEvents(port, prefix, path, destination, eventsToSend);
    collector.stop();
    Assert.assertEquals(eventsToSend,
        collector.getConsumer().eventsReceived());
    Assert.assertEquals(eventsToSend,
        collector.getConsumer().eventsSucceeded());
    Assert.assertEquals(0, collector.getConsumer().eventsFailed());
  }

  /**
   * This tests that the collector returns the correct HTTP codes for
   * invalid requests
   */
  @Test
  public void testBadRequests() throws Exception {
    // configure an collector
    final String name = "collect.rest";
    final String prefix = "/continuuity";
    final String path = "/stream/";
    final int port = TestUtil.findFreePort();

    CConfiguration configuration = new CConfiguration();
    configuration.set(Constants.CONFIG_CONNECTORS, name);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_CLASSNAME), RestCollector.class.getCanonicalName());
    configuration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), path);

    // create, configure, and start Accessor
    RestCollector collector = new RestCollector();
    collector.setName(name);
    collector.setConsumer(new TestUtil.NoopConsumer());
    collector.configure(configuration);
    collector.start();

    // the correct URL would be http://localhost:<port>/continuuity/destination/
    String baseUrl = collector.getHttpConfig().getBaseUrl();

    // submit a POST with flow/ or flow/stream as the destination -> 200
    Assert.assertEquals(200, TestUtil.sendPostRequest(baseUrl + "events/"));
    Assert.assertEquals(200, TestUtil.sendPostRequest(baseUrl + "events/more"));

    // submit a request without prefix in the path -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendPostRequest(
        "http://localhost:" + port + "/somewhere"));
    Assert.assertEquals(404, TestUtil.sendPostRequest(
        "http://localhost:" + port + "/continuuity/data"));

    // submit a request with correct prefix but no destination -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendPostRequest(baseUrl));

    // POST with destination but more after that in the path -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendPostRequest(
        baseUrl + "flow/stream/"));
    Assert.assertEquals(404, TestUtil.sendPostRequest(
        baseUrl + "flow/events/more"));

    // POST with existing key but with query part -> 501 Not Implemented
    Assert.assertEquals(501, TestUtil.sendPostRequest(
        baseUrl + "x?query=none"));

    // and shutdown
    collector.stop();
  }
}
