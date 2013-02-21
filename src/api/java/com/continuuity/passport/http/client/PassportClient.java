package com.continuuity.passport.http.client;

import com.continuuity.passport.meta.Account;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.*;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Client to communicate to the passport service.
 */

public class PassportClient {

  public final static String CONTINUUITY_API_KEY = "X-Continuuity-ApiKey";

  public final static String PASSPORT_SERVER_ADDRESS_KEY = "passport.server.address";

  public final static String PASSPORT_SERVER_PORT_KEY = "passport.server.port";


  private boolean debugEnabled = false;

  private static Cache<String, String> responseCache = null;
  private static Cache<String, Account> accountCache = null;

  /**
   * Enable debug - prints debug messages in std.out
   *
   * @param enableDebug true enables debugging. Default is false
   */
  public void enableDebug(boolean enableDebug) {
    this.debugEnabled = enableDebug;
  }

  public PassportClient() {
    //Cache valid responses from Servers for 10 mins
    responseCache = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .build();

    accountCache = CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(10,TimeUnit.MINUTES).build();
  }


  /**
   * Get List of VPC for the apiKey
   *
   * @param hostname Host of the service
   * @param apiKey   apiKey of the developer
   * @return List of VPC Names
   * @throws Exception RunTimeExceptions
   */
  public List<String> getVPCList(String hostname, int port,  String apiKey) throws RuntimeException {
    String url = getEndPoint(hostname,port, "passport/v1/vpc");
    //Check in cache- if present return it.
    List<String> vpcList = new ArrayList<String>();

    try {
      String data = responseCache.getIfPresent(apiKey);
      if (data == null) {
        data = httpGet(url, apiKey);

        if (data != null) {
          responseCache.put(apiKey, data);
        }
      }

      if (data != null) {
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(data);
        JsonArray jsonArray = element.getAsJsonArray();
        //for( )

        for (JsonElement elements : jsonArray) {
          JsonObject vpc = elements.getAsJsonObject();
          if (vpc.get("vpc_name") != null) {
            vpcList.add(vpc.get("vpc_name").getAsString());
          }
        }
      }
    }  catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
    return vpcList;

  }


  /**
   * Get List of VPC for the apiKey
   *
   * @param hostname Host of the service
   * @param apiKey   apiKey of the developer
   * @return Instance of {@Account}
   * @throws Exception RunTimeExceptions
   */
  public Account getAccount(String hostname, int port, String apiKey) throws RuntimeException {
    Preconditions.checkNotNull(hostname);
    String url = getEndPoint(hostname, port, "passport/v1/account/authenticate");
    Account account = null;

    try {
      account = accountCache.getIfPresent(apiKey);
      if (account == null) {
        String data = httpPost(url, apiKey);
        Gson gson  = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
        AuthenticateJson authJson = gson.fromJson(data, AuthenticateJson.class);
        account = authJson.getResult();
      }
      if(account != null) {
        accountCache.put(apiKey,account);
      }
      return account;
    }  catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }


  private String httpGet(String url, String apiKey) throws RuntimeException {
    String payload = null;
    HttpGet get = new HttpGet(url);
    get.addHeader(CONTINUUITY_API_KEY, apiKey);
    get.addHeader("X-Continuuity-Signature", "abcdef");

    if (debugEnabled) {
      System.out.println(String.format("Headers: %s ", get.getAllHeaders().toString()));
      System.out.println(String.format("URL: %s ", url));
      System.out.println(String.format("Method: %s ", "GET"));
    }

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    try {
      response = client.execute(get);
      payload = IOUtils.toString(response.getEntity().getContent());
      if (debugEnabled) {
        System.out.println(String.format("Response status: %d ", response.getStatusLine().getStatusCode()));
      }

      if(response.getStatusLine().getStatusCode()!=200){
        throw new RuntimeException(String.format("Call failed with status : %d",
          response.getStatusLine().getStatusCode()));
      }
    } catch (IOException e) {
      if (debugEnabled) {
        System.out.println(String.format("Caught exception while running http post call: Exception - %s", e.getMessage()));
        e.printStackTrace();
      }
      throw new RuntimeException(e);
    }
     finally {
      client.getConnectionManager().shutdown();

    }

    return payload;
  }

  private String httpPost(String url, String apiKey) throws RuntimeException {
    String payload = null;
    HttpPost get = new HttpPost(url);
    get.addHeader(CONTINUUITY_API_KEY, apiKey);
    get.addHeader("X-Continuuity-Signature", "abcdef");

    if (debugEnabled) {
      System.out.println(String.format("Headers: %s ", get.getAllHeaders().toString()));
      System.out.println(String.format("URL: %s ", url));
      System.out.println(String.format("Method: %s ", "POST"));
    }

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    try {
      response = client.execute(get);
      payload = IOUtils.toString(response.getEntity().getContent());

      if (debugEnabled) {
        System.out.println(String.format("Response status: %d ", response.getStatusLine().getStatusCode()));
        System.out.println(payload);
      }

      if(response.getStatusLine().getStatusCode()!=200){
        throw new RuntimeException(String.format("Call failed with status : %d",
          response.getStatusLine().getStatusCode()));
      }

    } catch (IOException e) {
      if (debugEnabled) {
        System.out.println(String.format("Caught exception while running http post call: Exception - %s", e.getMessage()));
        e.printStackTrace();
      }
      throw new RuntimeException(e);
    }
    finally{
      client.getConnectionManager().shutdown();
    }


    return payload;
  }


  private String getEndPoint(String hostname, int port, String endpoint) {
    return String.format("http://%s:%d/%s", hostname, port, endpoint);
  }

  private static class AuthenticateJson {
    private final String error;
    private final Account result;

    private AuthenticateJson(String error, Account result) {
      this.error = error;
      this.result = result;
    }

    public String getError() {
      return error;
    }

    public Account getResult() {
      return result;
    }
  }


}
