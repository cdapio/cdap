package com.continuuity.passport.httpendpoints;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.passport.Constants;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Organization;
import com.continuuity.passport.meta.VPC;
import com.continuuity.passport.testhelper.HyperSQL;
import com.continuuity.passport.testhelper.TestPassportServer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */

public class TestAccountHandler {

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
  public void accountCreate() throws IOException {
    String endPoint = String.format("http://localhost:%d/passport/v1/accounts", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(getAccountJson("sree@continuuity.com")));
    post.addHeader("Content-Type", "application/json");

    String result = TestPassportServer.request(post);
    assertTrue(result != null);
    Account account = Account.fromString(result);
    assertTrue("sree@continuuity.com".equals(account.getEmailId()));
  }


  @Test
  public void testAccounts() throws IOException, SQLException {
    String endPoint = String.format("http://localhost:%d/passport/v1/accounts", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(getAccountJson("john.smith@continuuity.com")));
    post.addHeader("Content-Type", "application/json");

    String result = TestPassportServer.request(post);
    assertTrue(result != null);
    Account account =  Account.fromString(result);
    assertTrue("john.smith@continuuity.com".equals(account.getEmailId()));
    int id = account.getAccountId();

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/%d/confirmed", port, id);
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity(getAccountJson("john.smith@continuuity.com", "john", "smith")));
    put.setHeader("Content-Type", "application/json");
    result = TestPassportServer.request(put);

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/%d/downloaded", port, id);
    put = new HttpPut(endPoint);
    result = TestPassportServer.request(put);
    assertTrue(result != null);

    account = Account.fromString(result);
    assertTrue("john.smith@continuuity.com".equals(account.getEmailId()));
    assertTrue("john".equals(account.getFirstName()));
    assertTrue("smith".equals(account.getLastName()));
    assertTrue(account.getApiKey() != null);
    String apiKey = account.getApiKey();

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/%d/confirmPayment", port, id);
    put = new HttpPut(endPoint);
    put.setEntity(new StringEntity("{\"payments_account_id\":\"12121\"}"));
    put.setHeader("Content-Type", "application/json");
    result = TestPassportServer.request(put);
    account = Account.fromString(result);
    assertTrue("john.smith@continuuity.com".equals(account.getEmailId()));
    assertTrue("12121".equals(account.getPaymentAccountId()));

    //testAccountRole
    endPoint = String.format("http://localhost:%d/passport/v1/accounts/%d/clusters", port, id);
    post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(getVPCJson("Classico", "Classico")));
    post.addHeader("Content-Type", "application/json");

    result = TestPassportServer.request(post);
    assertTrue(result != null);
    VPC vpc = VPC.fromString(result);
    assertTrue("Classico".equals(vpc.getVpcName()));
    assertTrue("sandbox".equals(vpc.getVpcType()));

    int vpcId = vpc.getVpcId();
    endPoint = String.format("http://localhost:%d/passport/v1/accounts", port);
    post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(getAccountJson("free@continuuity.com")));
    post.addHeader("Content-Type", "application/json");
    result = TestPassportServer.request(post);
    assertTrue(result != null);
    account =  Account.fromString(result);
    assertTrue("free@continuuity.com".equals(account.getEmailId()));

    int accountId = account.getAccountId();

    HyperSQL.insertIntoVPCRoleTable(vpcId, accountId);

    endPoint = String.format("http://localhost:%d/passport/v1/clusters/%s/accountRoles", port, vpc.getVpcName());
    HttpGet get = new HttpGet(endPoint);
    result = TestPassportServer.request(get);
    //TODO: Note there is some error with the Join query in Hypersql - the end point works against mysql
    assertTrue(result != null);
  }

  @Test
  public void testVPC() throws IOException {
    String endPoint = String.format("http://localhost:%d/passport/v1/accounts/0/clusters", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(getVPCJson("MyVPC", "MyVPC")));
    post.addHeader("Content-Type", "application/json");

    String result = TestPassportServer.request(post);
    assertTrue(result != null);
    VPC vpc = VPC.fromString(result);
    assertTrue("MyVPC".equals(vpc.getVpcName()));
    assertTrue("sandbox".equals(vpc.getVpcType()));
    int vpcId = vpc.getVpcId();

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/0/clusters/%d", port, vpcId);
    HttpGet get = new HttpGet(endPoint);

    result = TestPassportServer.request(get);
    assertTrue(result != null);
    vpc = VPC.fromString(result);
    assertTrue("MyVPC".equals(vpc.getVpcName()));
    assertTrue("MyVPC".equals(vpc.getVpcLabel()));
    assertTrue("sandbox".equals(vpc.getVpcType()));
 }

  @Test
  public void testOrganizationUpdate() throws IOException {

    //Create Org
    String endPoint = String.format("http://localhost:%d/passport/v1/organizations", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(TestPassportServer.getCompany("A123", "Amazon")));
    post.addHeader("Content-Type", "application/json");

    String result = TestPassportServer.request(post);
    assertTrue(result != null);
    Organization org =  Organization.fromString(result);
    assertTrue("A123".equals(org.getId()));
    assertTrue("Amazon".equals(org.getName()));

    //Create account
    endPoint = String.format("http://localhost:%d/passport/v1/accounts", port);
    post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(getAccountJson("joe.curry@continuuity.com")));
    post.addHeader("Content-Type", "application/json");

    result = TestPassportServer.request(post);
    assertTrue(result != null);
    Account account =  Account.fromString(result);
    assertTrue("joe.curry@continuuity.com".equals(account.getEmailId()));
    int id = account.getAccountId();

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/%d/confirmed", port, id);
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity(getAccountJson("joe.curry@continuuity.com", "joe", "curry")));
    put.setHeader("Content-Type", "application/json");
    result = TestPassportServer.request(put);

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/%d/organizations/%s", port, id, "A123");
    put = new HttpPut(endPoint);
    result = TestPassportServer.request(put);
    account =  Account.fromString(result);
    assertEquals(account.getOrgId(), "A123");
  }

  @Test
  public void testAccountRegistrationFlow() throws IOException {
    //Endpoints involved in account registration flow
    // POST accountRegistration/getNonce (get nonce for email_id)
    // GET accountRegistration/getId
    // POST account
    String emailId = "richard@dawkins.com";
    String firstName = "richard";
    String lastName = "dawkins";

    String endPoint = String.format("http://localhost:%d/passport/v1/accounts/register/%s/generateNonce",
                                    port, emailId);
    HttpPost post = new HttpPost(endPoint);

    String result = TestPassportServer.request(post);

    assertTrue(result != null);
    Gson gson = new GsonBuilder().create();
    NonceResult nonceResult = gson.fromJson(result, NonceResult.class);

    assertTrue(nonceResult.error == null);

    long nonce = nonceResult.result;

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/register/%d/getId", port, nonce);
    HttpGet get = new HttpGet(endPoint);
    result = TestPassportServer.request(get);
    IdResult idResult = gson.fromJson(result, IdResult.class);

    assertTrue(idResult.error == null);
    assertEquals(emailId, idResult.result);

    endPoint = String.format("http://localhost:%d/passport/v1/accounts", port);
    post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(getAccountJson(emailId)));
    post.addHeader("Content-Type", "application/json");

    result = TestPassportServer.request(post);
    assertTrue(result != null);
    Account account =  Account.fromString(result);

    assertEquals(emailId, account.getEmailId());

    int id = account.getAccountId();

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/%d/confirmed", port, id);
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity(getAccountJson(emailId, firstName, lastName)));
    put.setHeader("Content-Type", "application/json");
    result = TestPassportServer.request(put);
    account =  Account.fromString(result);

    assertEquals(emailId, account.getEmailId());
    assertEquals(firstName, account.getFirstName());
    assertEquals(lastName, account.getLastName());
    assertEquals(id, account.getAccountId());
  }

  @Test
  public void testPasswordResetFlow() throws Exception {
    //-> POST accountReset/generateKey/{email_id}  (returns Nonceid)
    //-> PUT  accountReset/password/{nonce} (change password)
    //-> POST account/authenticate  (authenticate with new password)
    String emailId = "douglas.adams@continuuity.com";
    String endPoint = String.format("http://localhost:%d/passport/v1/accounts", port);
    HttpPost post = new HttpPost(endPoint);
    post.setEntity(new StringEntity(getAccountJson(emailId)));
    post.addHeader("Content-Type", "application/json");

    String result = TestPassportServer.request(post);
    assertTrue(result != null);
    Account account =  Account.fromString(result);
    assertTrue("douglas.adams@continuuity.com".equals(account.getEmailId()));
    int id = account.getAccountId();

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/%d/confirmed", port, id);
    HttpPut put = new HttpPut(endPoint);
    put.setEntity(new StringEntity(getAccountJson(emailId, "douglas", "adams")));
    put.setHeader("Content-Type", "application/json");
    result = TestPassportServer.request(put);

    endPoint = String.format("http://localhost:%d/passport/v1/accounts/reset/%s/generateKey", port, emailId);
    post = new HttpPost(endPoint);
    result = TestPassportServer.request(post);

    Gson gson = new Gson();
    NonceResult nonceResult = gson.fromJson(result, NonceResult.class);

    assertTrue(nonceResult.error == null);
    long nonce = nonceResult.result;

    JsonObject object = new JsonObject();
    object.addProperty("password", "!@#");
    endPoint = String.format("http://localhost:%d/passport/v1/accounts/reset/%d/password", port, nonce);
    post  = new HttpPost(endPoint);
    post.setEntity(new StringEntity(object.toString()));
    post.setHeader("Content-Type", "application/json");
    result = TestPassportServer.request(post);

    assertTrue(result != null);

    //Authenticate with new password.
    endPoint = String.format("http://localhost:%d/passport/v1/accounts/authenticate", port);
    post = new HttpPost(endPoint);
    JsonObject auth  = new JsonObject();
    auth.addProperty("email_id", emailId);
    auth.addProperty("password", "!@#");
    post.setEntity(new StringEntity(auth.toString()));
    post.setHeader("Content-Type", "application/json");
    result = TestPassportServer.request(post);
  }

  private String getAccountJson(String emailId) {
    JsonObject object = new JsonObject();
    object.addProperty("email_id", emailId);
    return object.toString();
  }

  private String getAccountJson(String emailId, String firstName, String lastName) {
    JsonObject object = new JsonObject();
    object.addProperty("email_id", emailId);
    object.addProperty("first_name", firstName);
    object.addProperty("last_name", lastName);
    object.addProperty("password", "123");
    object.addProperty("company", "foo");
    return object.toString();
  }


  private String getVPCJson(String vpcName, String vpcLabel) {
    JsonObject object = new JsonObject();
    object.addProperty("vpc_name", vpcName);
    object.addProperty("vpc_label", vpcLabel);

    return object.toString();
  }

  private class NonceResult {
    private String error;
    private Long result;
  }

  private class IdResult {
    private String error;
    private String result;
  }

}
