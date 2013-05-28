package com.continuuity.gateway;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.Operation;
import com.continuuity.data.operation.Write;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.tools.StreamClient;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamClientTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(StreamClientTest.class);

  private OperationExecutor executor = null;
  Gateway gateway = null;

  String name = "access.rest";
  String prefix = "/continuuity";
  String path = "/stream/";
  int port = 0;

  CConfiguration configuration = null;

  /**
   * Set up our data fabric and insert some test key/value pairs.
   */
  @Before
  public void setupAppFabricAndGateway() throws Exception {
    StreamClient.debug = true;
    configuration = new CConfiguration();

    // Set up our Guice injections
    Injector injector = Guice.createInjector(new GatewayTestModule(configuration));
    this.executor = injector.getInstance(OperationExecutor.class);

    String[][] keyValues = {
        { "cat", "pfunk" }, // a simple key and value
        { "the cat", "pfunk" }, // a key with a blank
        { "k\u00eby", "v\u00e4l\u00fce" } // key and value with non-ascii chars
    };
    // create a batch of writes
    List<WriteOperation> operations = new ArrayList<WriteOperation>(keyValues.length);
    for (String[] kv : keyValues) {
      operations.add(new Write(kv[0].getBytes("ISO8859_1"), Operation.KV_COL,
          kv[1].getBytes("ISO8859_1")));
    }
    // execute the batch and ensure it was successful
    executor.commit(TestUtil.DEFAULT_CONTEXT, operations);

    // configure a gateway
    port = PortDetector.findFreePort();
    configuration.setBoolean(Constants.CONFIG_DO_SERVICE_DISCOVERY, false);
    configuration.set(Constants.CONFIG_CONNECTORS, name);
//    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_CLASSNAME),
//                      DataRestAccessor.class.getCanonicalName());
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_CLASSNAME),
                      RestCollector.class.getCanonicalName());
    configuration.setInt(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), path);

    // Now create our Gateway with a dummy consumer (we don't run collectors)
    // and make sure to pass the data fabric executor to the gateway.
    gateway = new Gateway();
    gateway.setExecutor(executor);
    gateway.setConsumer(new TestUtil.NoopConsumer());
    gateway.setDiscoveryServiceClient(injector.getInstance(DiscoveryServiceClient.class));
    gateway.start(null, configuration);
  }

  @After
  public void shutDownGateway() throws Exception {
    // and shut down
    gateway.stop(false);
  }

  /**
   * This tests the StreamClient command line tool for various combinations of
   * command line arguments. Note that this tool is a command line tool,
   * and it prints stuff on the console. That is not testable with this
   * unit test. Therefore we only test whether it succeeds or fails for
   * certain command line argument.
   *
   * @throws Exception if anything goes wrong
   */
  @Test
  public void testUsage() throws Exception {

    // argument combinations that should return success
    String[][] goodArgsList = {
        { "--help" }, // print help
        { "create", "--stream", "firststream" }, // create simple stream
//        { "id", "--stream", "firststream" }, // fetch from simple stream
//        { "send", "--stream", "firststream", "--body", "funk" }, // send event to stream
//        { "fetch", "--stream", "firststream", "--consumer", "firstconsumer" }, // fetch from simple stream
    };

    // argument combinations that should lead to failure
    String[][] badArgsList = {
        { },
        { "create", "firststre@m" }, // create stream with illegal name
        { "fetch", "--key" }, // no key
    };

    // test each good combination
    for (String[] args : goodArgsList) {
      LOG.info("Testing: " + Arrays.toString(args));
      Assert.assertNotNull(new StreamClient().execute(args, configuration));
    }
    // test each bad combination
    for (String[] args : badArgsList) {
      LOG.info("Testing: " + Arrays.toString(args));
      Assert.assertNull(new StreamClient().execute(args, configuration));
    }
  }

  @Test
  public void testCreateStream() throws OperationException {
    // write a value
    Assert.assertEquals("OK.", command("firststream", new String[] {
      "create"}));
    Assert.assertEquals("OK.", command("firststream", new String[] {
      "info"}));

  }

  private String command(String streamId, String[] args) {
    if (streamId != null) {
      args = Arrays.copyOf(args, args.length + 2);
      args[args.length - 2] = "--stream";
      args[args.length - 1] = streamId;
    }
    return new StreamClient().execute(args, configuration);
  }

}
