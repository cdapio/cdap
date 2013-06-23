package com.continuuity.passport.passportclient;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.passport.Constants;
import com.continuuity.passport.http.client.AccountProvider;
import com.continuuity.passport.http.client.PassportClient;
import com.continuuity.passport.testhelper.HyperSQL;
import com.continuuity.passport.testhelper.TestPassportServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

/**
 * Testing passport client.
 */
public class TestPassportClient {

  private static TestPassportServer server;
  private static int port;

  @BeforeClass
  public static void startServer() throws Exception {

    port = PortDetector.findFreePort();

    //Startup HSQL instance
    HyperSQL.startHsqlDB();

    CConfiguration configuration = CConfiguration.create();
    configuration.setInt(Constants.CFG_SERVER_PORT, port);

    String connectionString = "jdbc:hsqldb:mem:test;hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false" +
      "&user=sa&zeroDateTimeBehavior=convertToNull";

    configuration.set(Constants.CFG_JDBC_CONNECTION_STRING, connectionString);
    String profanePath = TestPassportClient.class.getResource("/ProfaneWords").getPath();

    configuration.set(Constants.CFG_PROFANE_WORDS_FILE_PATH, profanePath);
    server = new TestPassportServer(configuration);

    System.out.println("Starting server");
    server.start();
    Thread.sleep(1000);
    assertTrue(server.isStarted());
    addAccount();
  }

  @Test
  public void testValidAccount() throws URISyntaxException {
    PassportClient client = PassportClient.create(String.format("http://localhost:%d", port));
    AccountProvider accountProvider = client.getAccount("apiKey1");

    assert (accountProvider != null);
    assert ("0".equals(accountProvider.getAccountId()));
    assert ("john".equals(accountProvider.get().getFirstName()));
    assert ("smith".equals(accountProvider.get().getLastName()));
    assert ("apiKey1".equals(accountProvider.get().getApiKey()));

  }


  @Test(expected = RuntimeException.class)
  public void testInvalidAccount() throws URISyntaxException {
    PassportClient client = PassportClient.create(String.format("http://localhost:%d", port));
    AccountProvider accountProvider = client.getAccount("apiKey100");

  }

  @AfterClass
  public static void stopServer() throws Exception {
    server.stop();
    HyperSQL.stopHsqlDB();
  }

  private static void addAccount() throws IOException, SQLException {

    Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:test;" +
      "hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false", "sa", "");

    String insertAccount = "INSERT INTO account (first_name,last_name,email_id,company,password,api_key) VALUES " +
      "('john', 'smith', 'john@smith.com', 'continuuity', 'johnsecure', 'apiKey1')";
    connection.prepareStatement(insertAccount).execute();

  }
}
