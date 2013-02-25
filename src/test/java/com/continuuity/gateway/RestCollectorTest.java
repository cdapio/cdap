package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import com.continuuity.runtime.MetadataModules;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.http.client.methods.HttpPost;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;

public class RestCollectorTest {

  static Collector newCollector(String name) {
    RestCollector collector = new RestCollector();
    collector.setMetadataService(new DummyMDS());
    collector.setName(name);
    return collector;
  }

  static Collector newCollectorWithRealOpexAndMDS(String name) {
    Injector injector = Guice.createInjector(new MetadataModules() .getInMemoryModules(),
                                             new DataFabricModules().getInMemoryModules());
    OperationExecutor opex = injector.getInstance(OperationExecutor.class);
    Collector collector = new RestCollector();
    collector.setMetadataService(new MetadataService(opex));
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
    int port = PortDetector.findFreePort();
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
    String destination = "foo";
    int eventsToSend = 10;
    int port = PortDetector.findFreePort();
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
    collector.setAuthenticator(new NoAuthenticator());
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
    final int port = PortDetector.findFreePort();

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
    collector.setMetadataService(new DummyMDS());
    collector.setAuthenticator(new NoAuthenticator());
    collector.configure(configuration);
    collector.start();

    // the correct URL would be http://localhost:<port>/continuuity/destination/
    String baseUrl = collector.getHttpConfig().getBaseUrl();

    // test that ping works
    Assert.assertEquals(200, TestUtil.sendGetRequest(
        "http://localhost:" + port + "/ping"));

    // submit a POST with flow/ or flow/stream as the destination -> 200
    Assert.assertEquals(200, TestUtil.sendPostRequest(baseUrl + "events/"));
    Assert.assertEquals(404, TestUtil.sendPostRequest(baseUrl + "events/more"));

    // submit a request without prefix in the path -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendPostRequest(
        "http://localhost:" + port + "/somewhere"));
    Assert.assertEquals(404, TestUtil.sendPostRequest(
        "http://localhost:" + port + "/continuuity/data"));

    // submit a request with correct prefix but no destination -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendPostRequest(baseUrl));

    // POST with destination that is not registered
    Assert.assertEquals(200, TestUtil.sendPostRequest(
        baseUrl + "pfunk")); //correct
    Assert.assertEquals(404, TestUtil.sendPostRequest(
        baseUrl + "xyz")); // incorrect, not registered
    // make mds return exists=true for all streams, try again, should succeed
    ((DummyMDS)collector.getMetadataService()).allowAll();
    Assert.assertEquals(200, TestUtil.sendPostRequest(
        baseUrl + "xyz")); // incorrect, not registered

    // POST with destination but more after that in the path -> 404 Not Found
    Assert.assertEquals(404, TestUtil.sendPostRequest(
        baseUrl + "pfunk/stream/"));
    Assert.assertEquals(404, TestUtil.sendPostRequest(
        baseUrl + "pfunk/events/more"));

    // POST with existing key but with query part -> 501 Not Implemented
    Assert.assertEquals(501, TestUtil.sendPostRequest(
        baseUrl + "x?query=none"));

    // and shutdown
    collector.stop();
  }
  /**
   * verify that a new stream gets created
   */
  @Test
  public void testCreateStream() throws Exception {
    String name = "other";
    String prefix = "/stream/";
    String middle_path = "";
    String streamId = "firststream";

    int port = PortDetector.findFreePort();

    //url = "http://localhost:" + port + prefix + "/" + destination + "/"
    //i.e. "http://localhost:10000/stream/firststream/
    String url = "http://localhost:" + port + prefix + streamId + "/";

    Collector collector = newCollectorWithRealOpexAndMDS(name);

    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.
      buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    configuration.set(Constants.
      buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.
      buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), middle_path);

    collector.configure(configuration);
    collector.setConsumer(new TestUtil.VerifyConsumer(15, name, streamId));
    collector.setAuthenticator(new NoAuthenticator());

    Account account =
      new Account(collector.getAuthenticator()
                    .getAccountId(new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                         HttpMethod.GET,
                                                         url)));

    MetadataService mds = collector.getMetadataService();

    // clean up streams/queries in mds if there are leftovers from other tests
    for (Stream stream : mds.getStreams(account)) {
      Assert.assertTrue(mds.deleteStream(account, stream));
    }

    collector.start();

    //send Http request with REST command to create stream
    Assert.assertEquals(200, TestUtil.sendPutRequest(url));

    Stream stream = new Stream(streamId);
    stream.setName(streamId);

    //check through MDS if stream got created
    Stream existingStream = mds.getStream(account, stream);
    Assert.assertTrue(existingStream.isExists());

    //delete new stream through MDS
    Assert.assertTrue(mds.deleteStream(account, stream));

    //check if new stream got deleted
    existingStream = mds.getStream(account, stream);
    Assert.assertFalse(existingStream.isExists());

    collector.stop();
  }
  /**
   * verify that a new stream gets created
   */
  @Test
  public void testBadCreateStreamRequest() throws Exception {
    String name = "other";
    String prefix = "/stream/";
    String middle_path = "";
    String badStreamId = "my&stream";

    int port = PortDetector.findFreePort();

    //url = "http://localhost:" + port + prefix + "/" + destination + "/"
    //i.e. "http://localhost:10000/stream/firststream/
    String url = "http://localhost:" + port + prefix + badStreamId + "/";

    Collector collector = newCollectorWithRealOpexAndMDS(name);

    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.
      buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    configuration.set(Constants.
      buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.
      buildConnectorPropertyName(name, Constants.CONFIG_PATH_MIDDLE), middle_path);

    collector.configure(configuration);
    collector.setConsumer(new TestUtil.VerifyConsumer(15, name, badStreamId));
    collector.setAuthenticator(new NoAuthenticator());

    Account account =
      new Account(collector.getAuthenticator()
                    .getAccountId(new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                                         HttpMethod.GET,
                                                         url)));

    MetadataService mds = collector.getMetadataService();

    // clean up streams/queries in mds if there are leftovers from other tests
    for (Stream stream : mds.getStreams(account)) {
      Assert.assertTrue(mds.deleteStream(account, stream));
    }

    collector.start();

    //send Http request with REST command to try to create stream with bad name
    Assert.assertEquals(400, TestUtil.sendPutRequest(url));

    Stream stream = new Stream(badStreamId);
    stream.setName(badStreamId);

    //check through MDS that stream did not get created
    Stream existingStream = mds.getStream(account, stream);
    Assert.assertFalse(existingStream.isExists());

    collector.stop();
  }
}
