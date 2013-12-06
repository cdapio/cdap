package com.continuuity.passport.passportclient;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.passport.Constants;
import com.continuuity.passport.http.client.AccountProvider;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.passport.testhelper.HyperSQL;
import com.continuuity.passport.testhelper.TestAccountServer;
import com.continuuity.passport.testhelper.TestPassportServer;
import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Testing passport client.
 */
public class TestPassportClient {

  private static TestPassportServer passportServer;

  private static TestAccountServer testAccountServer;
  private static int accountServerPort;

  @BeforeClass
  public static void startServer() throws Exception {

    int passportServerPort = PortDetector.findFreePort();

    //Startup HSQL instance
    HyperSQL.startHsqlDB();

    CConfiguration configuration = CConfiguration.create();
    configuration.setInt(Constants.CFG_SERVER_PORT, passportServerPort);

    String connectionString = "jdbc:hsqldb:mem:test;hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false" +
      "&user=sa&zeroDateTimeBehavior=convertToNull";

    configuration.set(Constants.CFG_JDBC_CONNECTION_STRING, connectionString);
    String profanePath = TestPassportClient.class.getResource("/ProfaneWords").getPath();

    configuration.set(Constants.CFG_PROFANE_WORDS_FILE_PATH, profanePath);
    passportServer = new TestPassportServer(configuration);

    System.out.println("Starting passportServer");
    passportServer.start();

    testAccountServer = new TestAccountServer(passportServerPort);
    testAccountServer.startAndWait();
    accountServerPort = testAccountServer.getPort();

    Thread.sleep(1000);
    assertTrue(passportServer.isStarted());
    addData();
  }

  @Test
  public void testValidAccount() throws URISyntaxException {
    PassportClient client = PassportClient.create(String.format("http://localhost:%d", accountServerPort));
    AccountProvider accountProvider = client.getAccount("apiKey1");

    assert (accountProvider != null);
    assert ("0".equals(accountProvider.getAccountId()));
    assert ("john".equals(accountProvider.get().getFirstName()));
    assert ("smith".equals(accountProvider.get().getLastName()));
    assert ("apiKey1".equals(accountProvider.get().getApiKey()));

  }

  @Test
  public void testListVpc() throws URISyntaxException {
    PassportClient client = PassportClient.create(String.format("http://localhost:%d", accountServerPort));
    List<String> vpcList = client.getVPCList("apiKey1");
    Assert.assertEquals(ImmutableList.of("vpc1"), vpcList);

  }

  @Test(expected = RuntimeException.class)
  public void testInvalidAccount() throws URISyntaxException {
    PassportClient client = PassportClient.create(String.format("http://localhost:%d", accountServerPort));
    AccountProvider accountProvider = client.getAccount("apiKey100");

  }

  @AfterClass
  public static void stopServer() throws Exception {
    testAccountServer.stopAndWait();
    passportServer.stop();
    HyperSQL.stopHsqlDB();
  }

  private static void addData() throws IOException, SQLException {

    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:test;" +
      "hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false", "sa", "");

    String insertAccount = "INSERT INTO account (first_name,last_name,email_id,company,password,api_key) VALUES " +
      "('john', 'smith', 'john@smith.com', 'continuuity', 'johnsecure', 'apiKey1')";
    connection.prepareStatement(insertAccount).execute();

    String insertVpc = "INSERT INTO vpc_account (account_id,vpc_name,vpc_label) VALUES " +  "(0, 'vpc1', 'vpc1')";
    connection.prepareStatement(insertVpc).execute();
  }
}
