package com.continuuity.gateway;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.WriteOperation;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.DequeueResult;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.admin.GetGroupID;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.gateway.accessor.DatasetRestAccessor;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.continuuity.metadata.thrift.Stream;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;

/**
 * Tests Data Rest Accessor
 */
public class DatasetRestAccessorTest {

  static final OperationContext context = TestUtil.DEFAULT_CONTEXT;

  // this is the executor for all access to the data fabric
  private static OperationExecutor executor;

  // this is the location factory for access to the data fabric
  private static LocationFactory locationFactory;

  private static DataSetAccessor dataSetAccessor;

  private static TransactionSystemClient txSystemClient;

  // a meta data service
  private static MetadataService mds;

  // an instantiator
  private static DataSetInstantiatorFromMetaData instantiator;

  // the accessor to test
  private DatasetRestAccessor accessor;

  /**
   * Set up in-memory data fabric.
   */
  @BeforeClass
  public static void setup() {

    // Set up our Guice injections
    Injector injector = Guice.createInjector(new GatewayTestModule(new CConfiguration()));
    executor = injector.getInstance(OperationExecutor.class);
    locationFactory = injector.getInstance(LocationFactory.class);
    dataSetAccessor = injector.getInstance(DataSetAccessor.class);
    txSystemClient = injector.getInstance(TransactionSystemClient.class);
    mds = new MetadataService(executor);
    instantiator = new DataSetInstantiatorFromMetaData(executor, locationFactory,
                                                       dataSetAccessor, txSystemClient,
                                                       mds);
  } // end of setupGateway

  static Table newTable(String name) throws TException, MetadataServiceException {
    DataSetSpecification spec = new Table(name).configure();
    Dataset ds = new Dataset(spec.getName());
    ds.setName(spec.getName());
    ds.setType(spec.getType());
    ds.setSpecification(new Gson().toJson(spec));
    mds.assertDataset(new Account(context.getAccount()), ds);
    return instantiator.getDataSet(name, context);
  }

  static Transaction startTx(DataSetInstantiatorFromMetaData instantiator) throws Exception {
    Transaction tx = txSystemClient.start();
    for (TransactionAware txAware : instantiator.getInstantiator().getTransactionAware()) {
      txAware.startTx(tx);
    }
    return tx;
  }

  static void commitTx(DataSetInstantiatorFromMetaData instantiator, Transaction tx) throws Exception {
    for (TransactionAware txAware : instantiator.getInstantiator().getTransactionAware()) {
      txAware.commitTx();
    }
    txSystemClient.commit(tx);
  }

  /**
   * Create a new rest accessor with a given name and parameters.
   *
   * @param name   The name for the accessor
   * @param prefix The path prefix for the URI
   * @param middle The path middle for the URI
   * @return the accessor's base URL for REST requests
   */
  String setupAccessor(String name, String prefix, String middle)
      throws Exception {
    // bring up a new accessor
    DatasetRestAccessor restAccessor = new DatasetRestAccessor();
    restAccessor.setName(name);
    restAccessor.setAuthenticator(new NoAuthenticator());
    // find a free port
    int port = PortDetector.findFreePort();
    // configure it
    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name, Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), middle);
    restAccessor.configure(configuration);
    restAccessor.setExecutor(executor);
    restAccessor.setLocationFactory(locationFactory);
    restAccessor.setDataSetAccessor(dataSetAccessor);
    restAccessor.setTxSystemClient(txSystemClient);
    restAccessor.setMetadataService(mds);
    // start the accessor
    restAccessor.start();
    // all fine
    this.accessor = restAccessor;
    return "http://localhost:" + port + prefix + middle;
  }

  void assertRead(String prefix, int start, int end, String query) throws IOException {
    HttpGet get = new HttpGet(prefix + query);
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(get);
    client.getConnectionManager().shutdown();
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    Type stringMapType = new TypeToken<Map<String, String>>() {}.getType();
    Map<String, String> map = new Gson().fromJson(reader, stringMapType);
    Assert.assertEquals(end - start + 1, map.size());
    for (int i = start; i < end; i++) {
      Assert.assertEquals("v" + i, map.get("c" + i));
    }
  }
  void assertReadFails(String prefix, String query, int expected) throws IOException {
    HttpGet get = new HttpGet(prefix + query);
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(get);
    client.getConnectionManager().shutdown();
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testTableReads() throws Exception {
    String urlPrefix = setupAccessor("data", "", "/data/");
    Table t = newTable("tTR");
    // write a row with 10 cols c0...c9 with values v0..v9
    String row = "tTR10";
    byte[] rowKey = row.getBytes();
    byte[][] cols = new byte[10][];
    byte[][] vals = new byte[10][];
    for (int i = 0; i < 10; ++i) {
      cols[i] = ("c" + i).getBytes();
      vals[i] = ("v" + i).getBytes();
    }
    Transaction tx = startTx(instantiator);
    t.write(new Write(rowKey, cols, vals));
    commitTx(instantiator, tx);
    // now read back in various ways
    String queryPrefix = urlPrefix + "Table/" + t.getName() + "/" + row;
    assertRead(queryPrefix, 0, 9, ""); // all columns
    assertRead(queryPrefix, 5, 5, "?columns=c5"); // only c5
    assertRead(queryPrefix, 3, 5, "?columns=c5,c3,c4"); // only c3,c4, and c5
    assertRead(queryPrefix, 8, 9, "?columns=c8,c9,c10"); // only c8 and c9
    assertRead(queryPrefix, 0, 4, "?stop=c5"); // range up to exclusive 5
    assertRead(queryPrefix, 0, 2, "?stop=c5&limit=3"); // range up to exclusive 5, limit 3 -> c0..c2
    assertRead(queryPrefix, 0, 2, "?stop=c3&limit=5"); // range up to exclusive 3, limit 5 -> c0..c2
    assertRead(queryPrefix, 5, 9, "?start=c5"); // range starting at 5
    assertRead(queryPrefix, 5, 7, "?start=c5&limit=3"); // range starting at 5, limit 3 -> c5..c7
    assertRead(queryPrefix, 0, 4, "?limit=5"); // all limit 5 -> c0..c5
    assertRead(queryPrefix, 0, 9, "?limit=12"); // all limit 12 -> c0..c9
    assertRead(queryPrefix, 2, 5, "?start=c2&stop=c6"); // range from 2 to exclusive 6 -> 2,3,4,5
    assertRead(queryPrefix, 2, 4, "?start=c2&stop=c6&limit=3"); // range from 2 to exclusive 6 limited to 3 -> 2,3,4
    // now read some stuff that returns errors
    assertReadFails(queryPrefix, "/c1", HttpStatus.SC_BAD_REQUEST); // path does not end with row
    assertReadFails(queryPrefix, "?op=list", HttpStatus.SC_BAD_REQUEST); // list keys with row key
    assertReadFails(queryPrefix, "?columns=c1,c2&start=c5", HttpStatus.SC_BAD_REQUEST); // range and list of columns
    assertReadFails(queryPrefix, "?columns=c10&encoding=hex", HttpStatus.SC_BAD_REQUEST); // col invalid under encoding
    assertReadFails(queryPrefix, "?columns=c10&encoding=blah", HttpStatus.SC_BAD_REQUEST); // bad encoding
    assertReadFails(queryPrefix, "?columns=a", HttpStatus.SC_NOT_FOUND); // non-existing column
    assertReadFails(urlPrefix, "Table/" + t.getName() + "/abc", HttpStatus.SC_NOT_FOUND); // non-existing row
    assertReadFails(urlPrefix, "Table/abc/tTR10", HttpStatus.SC_NOT_FOUND); // non-existing table
    assertReadFails(urlPrefix, "Table/abc?op=list", HttpStatus.SC_NOT_IMPLEMENTED); // list keys not implemented
  }

  void assertWrite(String prefix, int expected, String query, String json) throws IOException {
    HttpPut put = new HttpPut(prefix + query);
    put.setEntity(new StringEntity(json, "UTF-8"));
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(put);
    client.getConnectionManager().shutdown();
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  void assertDelete(String prefix, int expected, String query) throws IOException {
    HttpDelete delete = new HttpDelete(prefix + query);
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(delete);
    client.getConnectionManager().shutdown();
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testTableWritesAndDeletes() throws Exception {
    String urlPrefix = setupAccessor("data", "", "/data/");
    Table t = newTable("tTW");
    String row = "abc";
    byte[] c1 = { 'c', '1' }, c2 = { 'c', '2' }, c3 = { 'c', '3' };
    byte[] v1 = { 'v', '1' }, mt = { }, v3 = { 'v', '3' };

    // write a row with 3 cols c1...c3 with values v1, "", v3
    String json = "{\"c1\":\"v1\",\"c2\":\"\",\"c3\":\"v3\"}";
    assertWrite(urlPrefix, HttpStatus.SC_OK, "Table/" + t.getName() + "/" + row, json);

    // starting new tx so that we see what was committed
    Transaction tx = startTx(instantiator);
    // read back directly and verify
    OperationResult<Map<byte[], byte[]>> result = t.read(new Read(row.getBytes()));
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(v1, result.getValue().get(c1));
    byte[] r2 = result.getValue().get(c2);
    Assert.assertTrue(null == r2 || Arrays.equals(mt, r2));
    Assert.assertArrayEquals(v3, result.getValue().get(c3));

    // delete c1 and c2
    assertDelete(urlPrefix, HttpStatus.SC_OK, "Table/" + t.getName() + "/" + row + "?columns=c1;columns=c2");

    // starting new tx so that we see what was committed
    commitTx(instantiator, tx);
    tx = startTx(instantiator);
    // read back directly and verify they're gone
    result = t.read(new Read(row.getBytes()));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(1, result.getValue().size());
    Assert.assertNull(result.getValue().get(c1));
    Assert.assertNull(result.getValue().get(c2));
    Assert.assertArrayEquals(v3, result.getValue().get(c3));

    // test some error cases
    assertWrite(urlPrefix, HttpStatus.SC_NOT_FOUND, "Table/abc/" + row, json); // non-existent table
    assertWrite(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/abc/a/x" + row, json); // path does not end with row
    assertWrite(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/abc?op=increment" + row, json); // put with increment
    assertWrite(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/abc/a?op=increment" + row, json); // put with increment
    assertWrite(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/" + t.getName() + "/" + row, ""); // no json
    assertWrite(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/" + t.getName() + "/" + row, "{\"\"}"); // wrong json
    // test errors for delete
    assertDelete(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/abc/" + row); // no columns specified
    assertDelete(urlPrefix, HttpStatus.SC_NOT_FOUND, "Table/abc/" + row + "?columns=a"); // non-existent table
    assertDelete(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/abc/a/x" + row); // path does not end with row
    assertDelete(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/abc?op=list" + row); // delete with operation
    assertDelete(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/abc/a?op=list" + row); // delete with op
    assertDelete(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/" + t.getName()); // no/empty row key
    assertDelete(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table//" + t.getName()); // no/empty row key
  }

  Map<String, Long> assertIncrement(String prefix, int expected, String query, String json) throws IOException {
    HttpPost post = new HttpPost(prefix + query);
    post.setEntity(new StringEntity(json, "UTF-8"));
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(post);
    client.getConnectionManager().shutdown();
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
    if (expected != HttpStatus.SC_OK) {
      return null;
    }
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    // JSon always returns string maps, no matter what the type, must be due to type erasure
    Type valueMapType = new TypeToken<Map<String, String>>(){}.getType();
    Map<String, String> map = new Gson().fromJson(reader, valueMapType);
    // convert to map(string->long)
    Map<String, Long> longMap = Maps.newHashMap();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      longMap.put(entry.getKey(), Long.parseLong(entry.getValue()));
    }
    return longMap;
  }

  @Test
  public void testIncrement() throws Exception {
    String urlPrefix = setupAccessor("data", "", "/data/");
    Table t = newTable("tI");
    String row = "abc";
    // directly write a row with two columns, a long, b not
    final byte[] a = { 'a' }, b = { 'b' }, c = { 'c' };
    Transaction tx = startTx(instantiator);
    t.write(new Write(row.getBytes(), new byte[][] { a, b }, new byte[][] { Bytes.toBytes(7L), b }));
    commitTx(instantiator, tx);

    // submit increment for row with c1 and c3, should succeed
    String json = "{\"a\":35, \"c\":11}";
    Map<String, Long> map = assertIncrement(urlPrefix, 200, "Table/" + t.getName() + "/" + row + "?op=increment", json);

    // starting new tx so that we see what was committed
    tx = startTx(instantiator);
    // verify result is the incremented value
    Assert.assertNotNull(map);
    Assert.assertEquals(2L, map.size());
    Assert.assertEquals(new Long(42), map.get("a"));
    Assert.assertEquals(new Long(11), map.get("c"));

    // verify directly incremented has happened
    OperationResult<Map<byte[], byte[]>> result = t.read(new Read(row.getBytes()));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(3, result.getValue().size());
    Assert.assertArrayEquals(Bytes.toBytes(42L), result.getValue().get(a));
    Assert.assertArrayEquals(b, result.getValue().get(b));
    Assert.assertArrayEquals(Bytes.toBytes(11L), result.getValue().get(c));

    // submit an increment for a and b, must fail with not-a-number
    json = "{\"a\":1,\"b\":12}";
    assertIncrement(urlPrefix, 400, "Table/" + t.getName() + "/" + row + "?op=increment", json);

    // starting new tx so that we see what was committed
    commitTx(instantiator, tx);
    tx = startTx(instantiator);
    // verify directly that the row is unchanged
    result = t.read(new Read(row.getBytes()));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(3, result.getValue().size());
    Assert.assertArrayEquals(Bytes.toBytes(42L), result.getValue().get(a));
    Assert.assertArrayEquals(b, result.getValue().get(b));
    Assert.assertArrayEquals(Bytes.toBytes(11L), result.getValue().get(c));

    // submit an increment for non-existent row, should succeed
    json = "{\"a\":1,\"b\":-12}";
    map = assertIncrement(urlPrefix, 200, "Table/" + t.getName() + "/xyz?op=increment", json);

    // starting new tx so that we see what was committed
    commitTx(instantiator, tx);
    tx = startTx(instantiator);
    // verify return value is equal to increments
    Assert.assertNotNull(map);
    Assert.assertEquals(2L, map.size());
    Assert.assertEquals(new Long(1), map.get("a"));
    Assert.assertEquals(new Long(-12), map.get("b"));
    // verify directly that new values are there
    // verify directly that the row is unchanged
    result = t.read(new Read("xyz".getBytes()));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(2, result.getValue().size());
    Assert.assertArrayEquals(Bytes.toBytes(1L), result.getValue().get(a));
    Assert.assertArrayEquals(Bytes.toBytes(-12L), result.getValue().get(b));

    // test some bad cases
    assertIncrement(urlPrefix, 400, "Table/" + t.getName() + "?op=increment", json); // no row key
    assertIncrement(urlPrefix, 400, "Table/" + t.getName() + "/?op=increment", json); // empty row key
    assertIncrement(urlPrefix, 404, "Table/" + t.getName() + "1/abc?op=increment", json); // table does not exist
    assertIncrement(urlPrefix, 400, "Table/" + t.getName() + "1/abc/x?op=increment", json); // path does not end on row
    assertIncrement(urlPrefix, 400, "Table/" + t.getName() + "/xyz?op=increment", "{\"a\":\"b\"}"); // json invalid
    assertIncrement(urlPrefix, 400, "Table/" + t.getName() + "/xyz?op=increment", "{\"a\":1"); // json invalid
  }

  void assertCreate(String prefix, int expected, String query) throws IOException {
    HttpPut put = new HttpPut(prefix + query);
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(put);
    client.getConnectionManager().shutdown();
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testCreateTable() throws Exception {
    String urlPrefix = setupAccessor("data", "", "/data/");
    urlPrefix += "Table/";
    String table = "tCTbl";
    String table2 = "tCTbl2";
    try {
      instantiator.getDataSet(table, context);
      Assert.fail("instantiator should have failed for non-existent table.");
    } catch (DataSetInstantiationException e) {
      // expected
    }
    assertCreate(urlPrefix, HttpStatus.SC_OK, table);
    assertWrite(urlPrefix, HttpStatus.SC_OK, table + "/abc", "{ \"c1\":\"v1\"}");
    // creating the table again should work (it is compatible with existing spec)
    assertCreate(urlPrefix, HttpStatus.SC_OK, table);
    assertWrite(urlPrefix, HttpStatus.SC_OK, table + "/abc", "{ \"c2\":\"v2\"}");
    // make sure both columns are there
    assertRead(urlPrefix, 1, 2, table + "/abc");
/*
   // todo: commented out because it looks incorrect: we use different names for spec and dataset. Very weird.
   //       Discuss it!

    // try to create a table that exists with a different dataset type
    DataSetSpecification spec = new KeyValueTable(table2).configure();
    Dataset ds = new Dataset(spec.getName());
    ds.setName(spec.getName());
    ds.setType(spec.getType());
    ds.setSpecification(new Gson().toJson(spec));
    mds.assertDataset(new Account(context.getAccount()), ds);
    // should now fail with conflict
    assertCreate(urlPrefix, HttpStatus.SC_CONFLICT, table2);

    // try some other bad requests
    assertCreate(urlPrefix, HttpStatus.SC_BAD_REQUEST, table + "?op=create"); // operation specified ->invalid
    assertCreate(urlPrefix, HttpStatus.SC_BAD_REQUEST, "" + "?op=create"); // empty table name
*/
  }

  @Test
  public void testTruncateTable() throws Exception {
    String urlPrefix = setupAccessor("data", "", "/data/");
    String tablePrefix = urlPrefix + "Table/";
    String table = "ttTbl";
    assertCreate(tablePrefix, HttpStatus.SC_OK, table);
    assertWrite(tablePrefix, HttpStatus.SC_OK, table + "/abc", "{ \"c1\":\"v1\"}");
    // make sure both columns are there
    assertRead(tablePrefix, 1, 1, table + "/abc");

    String dataSetManagementOpsPrefix = urlPrefix + "DataSet/";
    assertTruncate(dataSetManagementOpsPrefix, HttpStatus.SC_OK, table);

    // make sure data was removed: 404 on read
    assertReadFails(tablePrefix, table + "/abc", HttpStatus.SC_NOT_FOUND);

    // but table is there: we can write into it again
    assertCreate(tablePrefix, HttpStatus.SC_OK, table);
    assertWrite(tablePrefix, HttpStatus.SC_OK, table + "/abc", "{ \"c3\":\"v3\"}");
    // make sure both columns are there
    assertRead(tablePrefix, 3, 3, table + "/abc");
  }

  void assertTruncate(String prefix, int expected, String table) throws IOException {
    HttpDelete delete = new HttpDelete(prefix + table + "?op=truncate");
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(delete);
    client.getConnectionManager().shutdown();
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

  static final QueueEntry STREAM_ENTRY = new QueueEntry("x".getBytes());

  static WriteOperation addToStream(String name) {
    return new QueueEnqueue(("stream:" + name).getBytes(), STREAM_ENTRY);
  }

  static WriteOperation addToQueue(String name) {
    return new QueueEnqueue(("queue:" + name).getBytes(), STREAM_ENTRY);
  }

  static void createStream(String name) throws Exception {
    Stream stream = new Stream(name);
    stream.setName(name);
    mds.assertStream(new Account(context.getAccount()), stream);
    executor.commit(context, addToStream(name));
  }

  static void createQueue(String name) throws Exception {
    executor.commit(context, addToQueue(name));
  }

  static Write addToTable = new Write(new byte[] {'a'}, new byte[] {'b'}, new byte[] {'c'});

  static Table createTable(String name) throws Exception {
    Table table = newTable(name);
    Transaction tx = startTx(instantiator);
    table.write(addToTable);
    commitTx(instantiator, tx);
    return table;
  }

  boolean dequeueOne(String queue) throws Exception {
    long groupId = executor.execute(context, new GetGroupID(queue.getBytes()));
    QueueConsumer consumer = new QueueConsumer(0, groupId, 1,
                                               new QueueConfig(QueuePartitioner.PartitionerType.FIFO, true));
    executor.execute(context, new QueueConfigure(queue.getBytes(), consumer));
    DequeueResult result = executor.execute(context,
                                            new QueueDequeue(queue.getBytes(), consumer, consumer.getQueueConfig()));
    return !result.isEmpty();
  }

  boolean verifyStream(String name) throws Exception {
    Stream stream = mds.getStream(new Account(context.getAccount()), new Stream(name));
    boolean streamExists = stream.isExists();
    boolean dataExists = dequeueOne("stream:" + name);
    return streamExists || dataExists;
  }

  boolean verifyQueue(String name) throws Exception {
    return dequeueOne("queue:" + name);
  }

  boolean verifyTable(String name) throws Exception {
    OperationResult<Map<byte[], byte[]>> result;
    try {
      Table table = instantiator.getDataSet(name, context);
      Transaction tx = startTx(instantiator);
      result = table.read(new Read(new byte[]{'a'}, new byte[]{'b'}));
      commitTx(instantiator, tx);
    } catch (DataSetInstantiationException e) {
      result = executor.execute(
        context, new com.continuuity.data.operation.Read(name, new byte[]{'a'}, new byte[]{'b'}));
    }
    return !result.isEmpty();
  }

  @Test
  public void testClearData() throws Exception {
    // setup accessor
    setupAccessor("access.rest", "/continuuity", "/data/");
    String clearUrl = this.accessor.getHttpConfig().getBaseUrl() + "?clear=";

    String tableName = "mannamanna";
    String streamName = "doobdoobee";
    String queueName = "doobee";

    // create a stream, a queue, a table
    createTable(tableName);
    createStream(streamName);
    createQueue(queueName);

    // verify they are all there
    Assert.assertTrue(verifyTable(tableName));
    Assert.assertTrue(verifyStream(streamName));
    Assert.assertTrue(verifyQueue(queueName));

    // clear all
    Assert.assertEquals(200, TestUtil.sendPostRequest(clearUrl + "all"));
    // verify all are gone
    Assert.assertFalse(verifyTable(tableName));
    Assert.assertFalse(verifyStream(streamName));
    Assert.assertFalse(verifyQueue(queueName));
  }

  void assertRead(String prefix, String query, String col, String val) throws IOException {
    HttpGet get = new HttpGet(prefix + query);
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(get);
    client.getConnectionManager().shutdown();
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    Reader reader = new InputStreamReader(response.getEntity().getContent(), Charsets.UTF_8);
    Type stringMapType = new TypeToken<Map<String, String>>() {}.getType();
    Map<String, String> map = new Gson().fromJson(reader, stringMapType);
    Assert.assertEquals(1, map.size());
    Assert.assertEquals(val, map.get(col));
  }

  @Test
  public void testEncodingOfKeysAndValues() throws Exception {

    // first create the table
    String tableName = "tEOCAV";
    Table table = createTable(tableName);
    byte[] x = { 'x' }, y = { 'y' }, z = { 'z' }, a = { 'a' };

    // setup accessor
    String urlPrefix = setupAccessor("data", "/", "data/");
    String tablePrefix = urlPrefix + "Table/" + tableName + "/";

    // table is empty, write value z to column y of row x, use encoding "url"
    assertWrite(tablePrefix, HttpStatus.SC_OK, "%78" + "?encoding=url", "{\"%79\":\"%7A\"}");
    // read back directly and verify

    // starting new tx so that we see what was committed
    Transaction tx = startTx(instantiator);
    OperationResult<Map<byte[], byte[]>> result = table.read(new Read(x));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(1, result.getValue().size());
    Assert.assertArrayEquals(z, result.getValue().get(y));

    // read back with same encoding through REST - the response will not escape y or z
    assertRead(tablePrefix, "%78" + "?columns=%79" + "&encoding=url", "y", "z");
    // read back with hex encoding through REST - the response will escape y and z
    assertRead(tablePrefix, "78" + "?columns=79" + "&encoding=hex", "79", "7a");
    // read back with base64 encoding through REST - the response will escape y and z
    assertRead(tablePrefix, "eA" + "?columns=eQ" + "&encoding=base64", "eQ", "eg");

    // delete using hex encoding
    assertDelete(tablePrefix, 200, "78" + "?columns=79" + "&encoding=hex");

    // starting new tx so that we see what was committed
    commitTx(instantiator, tx);
    tx = startTx(instantiator);
    // and verify that it is really gone
    OperationResult<Map<byte[], byte[]>> read = table.read(new Read(x));
    Assert.assertTrue(read.isEmpty());

    // increment column using REST
    assertIncrement(tablePrefix, 200, "eA" + "?op=increment" + "&encoding=base64", "{\"YQ\":42}");
    // verify the value was written using the Table
    // starting new tx so that we see what was committed
    commitTx(instantiator, tx);
    tx = startTx(instantiator);
    result = table.read(new Read(x));
    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(1, result.getValue().size());
    Assert.assertArrayEquals(Bytes.toBytes(42L), result.getValue().get(a));
    // read back via REST with hex encoding
    assertRead(tablePrefix, "eA" + "?column=YQ" + "&encoding=base64", "YQ",
               Base64.encodeBase64URLSafeString(Bytes.toBytes(42L)));
  }

}

