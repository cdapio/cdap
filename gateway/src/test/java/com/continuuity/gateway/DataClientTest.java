package com.continuuity.gateway;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.gateway.tools.DataClient;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Tests data client.
 */
// TODO: Poorna, when you migrate test, remove this one
@Ignore
public class DataClientTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(DataClientTest.class);

  Gateway gateway = null;

  String name = "access.rest";
  String prefix = "/continuuity";
  String path = "/table/";
  int port = 0;

  CConfiguration configuration = null;

  /**
   * Set up our data fabric and insert some test key/value pairs.
   */
  @Before
  public void setupDataFabricAndGateway() throws Exception {
    DataClient.debug = true;
    configuration = new CConfiguration();

    // Set up our Guice injections
    Injector injector = Guice.createInjector(new GatewayTestModule(configuration));

    String[][] keyValues = {
        { "cat", "pfunk" }, // a simple key and value
        { "the cat", "pfunk" }, // a key with a blank
        { "k\u00eby", "v\u00e4l\u00fce" } // key and value with non-ascii chars
    };
    // TODO: Poorna, when you migrate test, uncomment
    // create a batch of writes
//    List<WriteOperation> operations = new ArrayList<WriteOperation>(keyValues.length);
//    for (String[] kv : keyValues) {
//      operations.add(new Write(kv[0].getBytes("ISO8859_1"), Operation.KV_COL,
//          kv[1].getBytes("ISO8859_1")));
//    }
    // execute the batch and ensure it was successful
//    executor.commit(TestUtil.DEFAULT_CONTEXT, operations);

    // configure a gateway
    port = PortDetector.findFreePort();
    configuration.setBoolean(Constants.CONFIG_DO_SERVICE_DISCOVERY, false);
    configuration.set(Constants.CONFIG_CONNECTORS, name);
    configuration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), path);

    // Now create our Gateway with a dummy consumer (we don't run collectors)
    // and make sure to pass the data fabric executor to the gateway.
    gateway = new Gateway();
    gateway.setConsumer(new TestUtil.NoopConsumer());
    gateway.setDiscoveryServiceClient(
        injector.getInstance(DiscoveryServiceClient.class));
    gateway.start(null, configuration);
  }

  @After
  public void shutDownGateway() throws Exception {
    // and shut down
    gateway.stop(false);
  }

  /**
   * This tests the DataClient tool for various combinations of
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
        { "clear", "--all" }
//        { "read", "--key", "cat" }, // simple key
//        { "read", "--key", "k\u00eby", "--encoding", "Latin1" }, // non-ascii
//                                                   // key with latin1 encoding
//        { "read", "--key", "636174", "--hex" }, // "cat" in hex notation
//        { "read", "--key", "6beb79", "--hex" }, // non-Ascii "këy" in hex
//                                                // notation
//        { "read", "--key", "cat", "--base",
//            "http://localhost:" + port + prefix + path }, // explicit base url
//        { "read", "--key", "cat", "--host", "localhost" }, // correct hostname
//        { "read", "--key", "cat", "--connector", name }, // valid connector name
//
//        { "list" },
//        { "list", "--url" },
//        { "list", "--hex" },
//        { "list", "--encoding", "Latin1" },
//
//        { "write", "--key", "pfunk", "--value", "the cat" },
//        { "write", "--key", "c\u00e4t", "--value",
//                "pf\u00fcnk", "--encoding", "Latin1" }, // non-Ascii cät=pfünk
//        { "write", "--key", "cafebabe", "--value", "deadbeef", "--hex" }, // hex
//
//        // delete the value just written
//        { "delete", "--key", "pfunk" },
//        { "delete", "--key", "c\u00e4t", "--encoding", "Latin1" },
//        { "delete", "--key", "cafebabe", "--hex" },

    };

    // argument combinations that should lead to failure
    String[][] badArgsList = {
        { },
        { "read", "--key" }, // no key
        { "read", "--garble" }, // invalid argument
        { "read", "--encoding" }, // missing argument
        { "read", "--key-file" }, // missing argument
        { "read", "--value-file" }, // missing argument
        { "read", "--base" }, // missing argument
        { "read", "--host" }, // missing argument
        { "read", "--connector" }, // missing argument
        { "read", "--connector", "fantasy.name" }, // invalid connector name
        { "read", "--key", "funk", "--hex" }, // non-hexadecimal key with --hex
        { "read", "--key", "babed", "--hex" }, // key of odd length with --hex
        { "read", "--key", "pfunk", "--encoding", "fantasy string" }, // invalid
                                                                     // encoding
        { "read", "--key", "k\u00eby", "--ascii" }, // non-ascii key with
                              // --ascii. Note that this drops the msb of the ë
                              // and hence uses "key" as the key -> 404
        { "read", "--key", "key with blanks", "--url" }, // url-encoded key may
                                                          // not contain blanks
        { "read", "--key", "cat", "--base",
            "http://localhost" + prefix + path }, // explicit but port is
                                              // missing -> connection refused
        { "read", "--key", "cat", "--base",
            "http://localhost:" + port + "/gataca" + path }, // explicit but
                                                            // wrong base -> 404
        { "read", "--key", "cat", "--host", "my.fantasy.hostname" }, // bad host
                                                                  // name -> 404
        { "read", "--host", "localhost" }, // no key given

        { "list", "--encoding" }, // missing encoding
        { "list", "--key", "pfunk" }, // key not allowed
        { "list", "--value", "the cat" }, // value not allowed for list

        { "delete" }, // key missing
        { "delete", "--key", "pfunks", "--hex" }, // not a hex string
        { "delete", "--key", "cafebab", "--hex" }, // not a hex string
        { "delete", "--key", "cafe babe", "--url" }, // url string can't have
                                                     // blank
        { "delete", "--value", "cafe babe" }, // can't delete by value

    };

    // test each good combination
    for (String[] args : goodArgsList) {
      LOG.info("Testing: " + Arrays.toString(args));
      Assert.assertNotNull(new DataClient().execute(args, configuration));
    }
    // test each bad combination
    for (String[] args : badArgsList) {
      LOG.info("Testing: " + Arrays.toString(args));
      Assert.assertNull(new DataClient().execute(args, configuration));
    }
  }

  @Test @Ignore
  public void testValueAsCounter() throws OperationException {
    Assert.assertEquals("OK.", new DataClient().execute(new String[] {
        "write", "--key", "mycount", "--counter", "--value", "41" },
        configuration));
    // TODO: Poorna, when you migrate test, uncomment
//    Increment increment = new Increment("mycount".getBytes(), Operation.KV_COL, 1);
//    this.executor.increment(TestUtil.DEFAULT_CONTEXT, increment);
    Assert.assertEquals("42", new DataClient().execute(new String[] {
        "read", "--key", "mycount", "--counter" }, configuration));
  }

  String command(String table, String[] args) {
    if (table != null) {
      args = Arrays.copyOf(args, args.length + 2);
      args[args.length - 2] = "--table";
      args[args.length - 1] = table;
    }
    return new DataClient().execute(args, configuration);
  }

  void testReadWriteListDelete(String table) {
    String key1 = "real";
    String value1 = "data";
    String key2 = "big";
    String value2 = "time";
    // clear everything for clean state
    Assert.assertEquals("OK.", command(null, new String[] {
        "clear", "--all"}));
    // write a value
    Assert.assertEquals("OK.", command(table, new String[] {
        "write", "--key", key1, "--value", value1}));
    // write another value
    Assert.assertEquals("OK.", command(table, new String[] {
        "write", "--key", key2, "--value", value2}));
    // list keys and make sure it contains the exact two keys
    String listResult = command(table, new String[] {"list"});
    // result should be "9 bytes written"
    Assert.assertEquals(listResult.charAt(0) - '0',
        (key1 + "\n" + key2 + "\n").length());
    // verify the values
    Assert.assertEquals(value1, command(table, new String[]{
        "read", "--key", key1}));
    Assert.assertEquals(value2, command(table, new String[] {
        "read", "--key", key2}));
    // delete one key
    Assert.assertEquals("OK.", command(table, new String[] {
        "delete", "--key", key1}));
    // list keys again and make sure the deleted key is gone
    listResult = command(table, new String[] {"list"});
    // result should be "4 bytes written"
    Assert.assertEquals(listResult.charAt(0) - '0', (key2 + "\n").length());
  }

  @Test @Ignore
  public void testKeyValueTables() {
    testReadWriteListDelete(null);
    testReadWriteListDelete("mytable");
  }

}
