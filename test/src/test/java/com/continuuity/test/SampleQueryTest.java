package com.continuuity.test;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.util.Bytes;
import com.continuuity.api.query.QueryProvider;
import com.continuuity.api.query.QueryProviderContentType;
import com.continuuity.api.query.QueryProviderResponse;
import com.continuuity.api.query.QuerySpecifier;
import com.google.common.collect.ImmutableMultimap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class SampleQueryTest extends AppFabricTestBase {

  /**
   * Hello World! QueryProvider that responds with the method
   * and arguments passed to it as string.
   */
  public static class HelloWorldQueryProvider extends QueryProvider {
    @Override
    public void configure(QuerySpecifier specifier) {
      specifier.service("HelloWorld");
      specifier.dataset("simple");
      specifier.type(QueryProviderContentType.TEXT);
      specifier.provider(HelloWorldQueryProvider.class);
    }

    KeyValueTable kvTable;
    static final byte[] row = Bytes.toBytes("lastcall");

    @Override
    public void initialize() {
      super.initialize();
      kvTable = getQueryProviderContext().getDataSet("simple");
    }

    @Override
    public QueryProviderResponse process(String method, Map<String, String> arguments) {
      try {
        kvTable.exec(new KeyValueTable.WriteKey(row, Bytes.toBytes(method)));
      } catch (OperationException e) {
        return new QueryProviderResponse(QueryProviderResponse.Status.FAILED, "Failed.", e.getMessage());
      }
      StringBuffer sb = new StringBuffer();
      sb.append("method : ").append(method).append(" [ ");
      for(Map.Entry<String, String> argument : arguments.entrySet()) {
        sb.append(argument.getKey()).append("=").append(argument.getValue());
      }
      sb.append(" ] ");
      return new QueryProviderResponse(sb.toString());
    }
  }

  @Test//(timeout = 20000)
  public void testQueryProvider() throws Exception {

    // register the key value table used by the query provider
    // normally, this would be done by the app fabric when the application gets deployed
    registerDataSet(new KeyValueTable("simple"));

    // start the query provider
    TestQueryHandle queryHandle = startQuery(HelloWorldQueryProvider.class);
    assertTrue(queryHandle.isRunning());

    // submit a query, verif that it returns correct status and content
    QueryResult queryResult = runQuery(queryHandle, "xyz", ImmutableMultimap.<String, String>of("a","b"));
    Assert.assertEquals(200, queryResult.getReturnCode());
    Assert.assertEquals("method : xyz [ a=b ] ", queryResult.getContent());

    // get a runtime instance of the data set and read back the row written by the query provider
    KeyValueTable kv = getDataSet("simple");
    Assert.assertArrayEquals(Bytes.toBytes("xyz"), kv.read(HelloWorldQueryProvider.row));
  }

}