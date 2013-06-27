package com.continuuity.passport.httpendpoints;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.passport.Constants;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Organization;
import com.continuuity.passport.testhelper.HyperSQL;
import com.continuuity.passport.testhelper.TestPassportServer;
import com.google.gson.JsonObject;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Test Organization Handlers.
 */
public class TestOrganizationHandler {

  private static TestPassportServer server;
  private static int port;

  @BeforeClass
  public static void setup() throws Exception {

    port = PortDetector.findFreePort();

    //Startup HSQL instance
    HyperSQL.startHsqlDB();

    CConfiguration configuration = CConfiguration.create();
    configuration.setInt(Constants.CFG_SERVER_PORT, port);

    String connectionString = "jdbc:hsqldb:mem:test;hsqldb.default_table_type=cached;hsqldb.write_delay=false;" +
      "hsqldb.sql.enforce_size=false&user=sa&zeroDateTimeBehavior=convertToNull&autocommit=true";

    configuration.set(Constants.CFG_JDBC_CONNECTION_STRING, connectionString);
    String profanePath = TestAccountHandler.class.getResource("/ProfaneWords").getPath();

    configuration.set(Constants.CFG_PROFANE_WORDS_FILE_PATH, profanePath);
    server = new TestPassportServer(configuration);

    System.out.println("Starting server");
    server.start();
    Thread.sleep(1000);
    assertTrue(server.isStarted());
  }

  @AfterClass
  public static void teardown() throws Exception {
    server.stop();
    HyperSQL.stopHsqlDB();
  }

  @Test
  public void orgCreate() throws IOException {
    String endPoint = String.format("http://localhost:%d/passport/v1/organization", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(getCompany("C123", "Continuuity")));
    post.addHeader("Content-Type", "application/json");

    String result = TestPassportServer.request(post);
    assertTrue(result != null);
    Organization org =  Organization.fromString(result);
    assertTrue("C123".equals(org.getId()));
    assertTrue("Continuuity".equals(org.getName()));
  }

  private String getCompany(String id, String name){
    JsonObject object = new JsonObject();
    object.addProperty("id", id);
    object.addProperty("name", name);
    return object.toString();
  }

}
