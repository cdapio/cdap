package com.continuuity.passport.http.client;

import com.continuuity.common.utils.ImmutablePair;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.gson.*;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.DefaultHttpClient;
import org.mortbay.jetty.HttpStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *   Client to communicate to the passport service.
 */

//TODO: Thread safety?
public class PassportClient {

  public final static String CONTINUUITY_API_KEY = "X-Continuuity-ApiKey";

  private boolean debugEnabled = false;

  private static Cache<String, String> responseCache = null;

  /**
   * Enable debug - prints debug messages in std.out
   * @param enableDebug true enables debugging. Default is false
   */
  public void enableDebug(boolean enableDebug) {
    this.debugEnabled = enableDebug;
  }

  public PassportClient (){
    responseCache = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .build();
  }


  /**
   * Get List of VPC for the apiKey
   * @param hostname Host of the service
   * @param apiKey apiKey of the developer
   * @return  List of VPC Names
   * @throws Exception RunTimeExceptions
   */
  public List<String> getVPCList(String hostname, String apiKey) throws Exception {
    String url  = getEndPoint(hostname,"passport/v1/vpc");
    //Check in cache- if present return it.
    List<String> vpcList = new ArrayList<String>();

    try {
      String data = responseCache.getIfPresent(apiKey);
      if (data == null ) {
        data = httpGet(url,apiKey);

        if( data != null ) {
          responseCache.put(apiKey,data);
        }
      }

      if (data!= null ) {
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(data);
        JsonArray jsonArray = element.getAsJsonArray();
        //for( )

        for (JsonElement elements : jsonArray) {
          JsonObject vpc = elements.getAsJsonObject();
          if (vpc.get("vpc_name") != null ) {
            vpcList.add(vpc.get("vpc_name").getAsString());
          }
        }
      }
    }

    catch (IOException e) {
      e.printStackTrace();;
      throw new RuntimeException(e.getMessage());
    }
    catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
    return vpcList;

  }

  private String httpGet(String url,String apiKey ) throws Exception {
    String payload  = null;
    HttpGet get = new HttpGet(url);
    get.addHeader(CONTINUUITY_API_KEY,apiKey);
    get.addHeader("X-Continuuity-Signature","abcdef");

    if (debugEnabled) {
      System.out.println(String.format ("Headers: %s ",get.getAllHeaders().toString()));
      System.out.println(String.format ("URL: %s ",url));
      System.out.println(String.format("Method: %s ", "GET"));
    }

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    try {
      response = client.execute(get);
      payload = IOUtils.toString(response.getEntity().getContent());
      client.getConnectionManager().shutdown();
      if (debugEnabled) {
        System.out.println(String.format("Response status: %d ", response.getStatusLine().getStatusCode()));
      }
    } catch (IOException e) {
      if (debugEnabled)  {
        System.out.println(String.format("Caught exception while running http post call: Exception - %s",e.getMessage()));
        e.printStackTrace();
      }
      throw new RuntimeException(e);
    }


    return payload;
  }


  private String getEndPoint(String hostname, String endpoint){
    return String.format("http://%s:7777/%s",hostname,endpoint);
  }


}
