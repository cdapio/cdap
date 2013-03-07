package com.continuuity.passport.httpendpoints;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.passport.Constants;
import com.continuuity.passport.testhelper.HyperSQL;
import com.continuuity.passport.testhelper.TestPassportServer;
import com.google.gson.Gson;
import org.apache.http.client.methods.HttpPost;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 *
 */

public class HttpEndPointSuite  {

  private static TestPassportServer server;
  private static int port;

  @BeforeClass
  public static void setup() throws Exception {

    port = PortDetector.findFreePort();

    //Startup HSQL instance
    HyperSQL.startHsqlDB();

    CConfiguration configuration = CConfiguration.create();
    configuration.setInt(Constants.CFG_SERVER_PORT, port);

    String connectionString = "jdbc:hsqldb:mem:test;hsqldb.default_table_type=cached;hsqldb.sql.enforce_size=false" +
      "&user=sa&zeroDateTimeBehavior=convertToNull";

    configuration.set(Constants.CFG_JDBC_CONNECTION_STRING, connectionString);
    String profanePath = HttpEndPointSuite.class.getResource("/ProfaneWords").getPath();

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

  public void accountCreate() {
    HttpPost post = getPostRequest()

  }

  HttpPost getPostRequest(String endPoint, Gson data){
    HttpPost post = new HttpPost(getRequestURL(endPoint));
    post.setEntity();
  }

  private String getRequestURL(String endPoint ) {
    return String.format("http://localhost:%d/%s",port,endPoint);
  }

  @Test
  public void testAll() {
    accountCreate();
  }

}

