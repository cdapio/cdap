package com.continuuity.gateway.v2.handlers.v2.dataset;

import com.continuuity.gateway.GatewayFastTestsSuite;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test DatasetHandler.
 */
public class DatasetHandlerTest {

  @Test
  public void testTruncateTable() throws Exception {
    String urlPrefix = "/v2";
    String tablePrefix = urlPrefix + "/tables/";
    String table = "ttTbl_" + System.nanoTime();
    TableHandlerTest.assertCreate(tablePrefix, HttpStatus.SC_OK, table);
    TableHandlerTest.assertWrite(tablePrefix, HttpStatus.SC_OK, table + "/rows/abc", "{ \"c1\":\"v1\"}");
    // make sure both columns are there
    TableHandlerTest.assertRead(tablePrefix, 1, 1, table + "/rows/abc");

    assertTruncate(urlPrefix, HttpStatus.SC_OK, "/datasets/" + table + "/truncate");

    // make sure data was removed: 204 on read
    TableHandlerTest.assertReadFails(tablePrefix, table + "/rows/abc", HttpStatus.SC_NO_CONTENT);

    // but table is there: we can write into it again
    TableHandlerTest.assertCreate(tablePrefix, HttpStatus.SC_OK, table);
    TableHandlerTest.assertWrite(tablePrefix, HttpStatus.SC_OK, table + "/rows/abc", "{ \"c3\":\"v3\"}");
    // make sure both columns are there
    TableHandlerTest.assertRead(tablePrefix, 3, 3, table + "/rows/abc");
  }

  void assertTruncate(String prefix, int expected, String query) throws Exception {
    HttpResponse response = GatewayFastTestsSuite.doPost(prefix + query, "");
    Assert.assertEquals(expected, response.getStatusLine().getStatusCode());
  }

}
