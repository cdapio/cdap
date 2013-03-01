package com.continuuity.gateway;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.gateway.accessor.DatasetRestAccessor;
import com.continuuity.gateway.auth.NoAuthenticator;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.consumer.StreamEventWritingConsumer;
import com.continuuity.gateway.util.DataSetInstantiatorFromMetaData;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
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

public class DatasetRestAccessorTest {

  static final OperationContext context = OperationContext.DEFAULT;

  // this is the executor for all access to the data fabric
  private static OperationExecutor executor;

  // a meta data service
  private static MetadataService mds;

  // an instantiator
  private static DataSetInstantiatorFromMetaData instantiator;

  // the accessor to test
  private DatasetRestAccessor accessor;

  // the rest collector we will use in the clear test
  private RestCollector collector;

  /**
   * Set up in-memory data fabric
   */
  @BeforeClass
  public static void setup() {

    // Set up our Guice injections
    Injector injector = Guice.createInjector(
        new DataFabricModules().getInMemoryModules());
    executor = injector.getInstance(OperationExecutor.class);
    mds = new MetadataService(executor);
    instantiator = new DataSetInstantiatorFromMetaData(executor, mds);

  } // end of setupGateway


  static Table createTable(String name) throws TException, MetadataServiceException {
    DataSetSpecification spec = new Table(name).configure();
    Dataset ds = new Dataset(spec.getName());
    ds.setName(spec.getName());
    ds.setType(spec.getType());
    ds.setSpecification(new Gson().toJson(spec));
    mds.assertDataset(new Account(context.getAccount()), ds);
    return instantiator.getDataSet(name, context);
  }

  /**
   * Create a new rest accessor with a given name and parameters
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
    restAccessor.setMetadataService(mds);
    // start the accessor
    restAccessor.start();
    // all fine
    this.accessor = restAccessor;
    return "http://localhost:" + port + prefix + middle;
  }

  // we will need this to test the clear API
  String setupCollector(String name, String prefix, String middle)
      throws Exception {
    // bring up a new collector
    RestCollector restCollector = new RestCollector();
    restCollector.setName(name);
    restCollector.setAuthenticator(new NoAuthenticator());
    // find a free port
    int port = PortDetector.findFreePort();
    // configure it
    CConfiguration configuration = new CConfiguration();
    configuration.setInt(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PORT), port);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_PREFIX), prefix);
    configuration.set(Constants.buildConnectorPropertyName(name,
        Constants.CONFIG_PATH_MIDDLE), middle);
    restCollector.configure(configuration);

    StreamEventWritingConsumer consumer = new StreamEventWritingConsumer();
    consumer.setExecutor(this.executor);
    restCollector.setConsumer(consumer);
    restCollector.setExecutor(this.executor);
    restCollector.setMetadataService(new DummyMDS());
    // start the accessor
    restCollector.start();
    // all fine
    this.collector = restCollector;
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
    Table t = createTable("tTR");
    // write a row with 10 cols c0...c9 with values v0..v9
    byte[] rowKey = "tTR10".getBytes();
    byte[][] cols = new byte[10][];
    byte[][] vals = new byte[10][];
    for (int i = 0; i < 10; ++i) {
      cols[i] = ("c" + i).getBytes();
      vals[i] = ("v" + i).getBytes();
    }
    t.write(new Write(rowKey, cols, vals));
    // now read back in various ways
    String queryPrefix = urlPrefix + "Table/" + t.getName() + "/" + "tVR10";
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
    Table t = createTable("tTW");
    String row = "abc";
    byte[] c1 = { 'c', '1' }, c2 = { 'c', '2' }, c3 = { 'c', '3' };
    byte[] v1 = { 'v', '1' }, mt = { }, v3 = { 'v', '3' };

    // write a row with 3 cols c1...c3 with values v1, "", v3
    String json = "{\"c1\":\"v1\",\"c2\":\"\",\"c3\":\"v3\"}";
    assertWrite(urlPrefix, HttpStatus.SC_OK, "Table/" + t.getName() + "/" + row, json);
    // read back directly and verify
    OperationResult<Map<byte[], byte[]>> result = t.read(new Read(row.getBytes()));
    Assert.assertFalse(result.isEmpty());
    Assert.assertArrayEquals(v1, result.getValue().get(c1));
    byte[] r2 = result.getValue().get(c2);
    Assert.assertTrue(null == r2 || Arrays.equals(mt, r2));
    Assert.assertArrayEquals(v3, result.getValue().get(c3));

    // delete c1 and c2
    assertDelete(urlPrefix, HttpStatus.SC_OK, "Table/" + t.getName() + "/" + row + "?columns=c1;columns=c2");
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
    assertWrite(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table/" + t.getName(), json); // no/empty row key
    assertWrite(urlPrefix, HttpStatus.SC_BAD_REQUEST, "Table//" + t.getName(), json); // no/empty row key
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
    if (expected != HttpStatus.SC_OK) return null;
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
    Table t = createTable("tI");
    String row = "abc";
    // directly write a row with two columns, a long, b not
    final byte[] a = { 'a' }, b = { 'b' }, c = { 'c' };
    t.write(new Write(row.getBytes(), new byte[][] { a, b }, new byte[][] { Bytes.toBytes(7L), b }));

    // submit increment for row with c1 and c3, should succeed
    String json = "{\"a\":35,\"c\":11}";
    Map<String,Long> map = assertIncrement(urlPrefix, 200, "Table/" + t.getName() + "/" + row + "?op=increment", json);
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
}

