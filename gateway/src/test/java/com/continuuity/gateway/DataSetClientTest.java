package com.continuuity.gateway;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.tools.DataSetClient;
import org.junit.Assert;
import org.junit.Test;

/**
 * Data set client tests.
 */
public class DataSetClientTest {
  private static final String hostname = "127.0.0.1";

  @Test
  public void testDataSetOps() throws OperationException {
    CConfiguration configuration = CConfiguration.create();
    String port = Integer.toString(GatewayFastTestsSuite.getPort());
    configuration.set(Constants.Gateway.ADDRESS, hostname);
    configuration.set(Constants.Gateway.PORT, port);

    final String table = "tVAC";
    final String row = "myRow";
    final String column = "myCounter";
    Assert.assertEquals("OK.", new DataSetClient().execute(new String[] {
      "create", "--table", table }, configuration));
    Assert.assertEquals("OK.", new DataSetClient().execute(new String[] {
      "write", "--table", table, "--row", row, "--column", column, "--value", "41", "--counter" }, configuration));
    Assert.assertEquals("42", new DataSetClient().execute(new String[] {
      "increment", "--table", table, "--row", row, "--column", column, "--value", "1" }, configuration));
    Assert.assertEquals("42", new DataSetClient().execute(new String[] {
      "read", "--table", table, "--row", row, "--column", column, "--counter" }, configuration));
  }

  @Test
  public void testClearDataset() throws OperationException {
    CConfiguration configuration = CConfiguration.create();
    String port = Integer.toString(GatewayFastTestsSuite.getPort());

    final String table = "tVACLr";
    final String row = "myRow";
    final String column = "myCounter";
    Assert.assertEquals("OK.", new DataSetClient().execute(new String[] {
      "create", "--table", table, "--host", hostname, "--port", port }, configuration));
    Assert.assertEquals("OK.", new DataSetClient().execute(new String[] {
      "write", "--table", table, "--row", row, "--column", column, "--value", "41", "--counter",
      "--host", hostname, "--port", port }, configuration));
    Assert.assertEquals("42", new DataSetClient().execute(new String[] {
      "increment", "--table", table, "--row", row, "--column", column, "--value", "1",
      "--host", hostname, "--port", port }, configuration));

    configuration.set(Constants.Gateway.ADDRESS, hostname);
    configuration.set(Constants.Gateway.PORT, port);
    Assert.assertEquals("OK.", new DataSetClient().execute(new String[]{
      "clear", "--datasets"}, configuration));
    Assert.assertNull(new DataSetClient().execute(new String[]{
      "read", "--table", table, "--row", row, "--column", column, "--counter"}, configuration));
  }
}
