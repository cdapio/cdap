package com.continuuity.passport.http.client;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;

/**
 *   Client to communicate to the passport service.
 */



public class PassportClient {

  private final static String CONTINUUITY_API_KEY = "X-Continuuity-ApiKey";

  private boolean debugEnabled = false;

  public void enableDebug(boolean enableDebug) {
    this.debugEnabled = enableDebug;
  }


  private HttpResponse httpPut(String url,String apiKey, String body ) throws Exception {

    HttpPut put = new HttpPut(url);
    put.addHeader(CONTINUUITY_API_KEY,apiKey);

    if (debugEnabled) {
      System.out.println(String.format ("Headers: %s ",put.getAllHeaders().toString()));
      System.out.println(String.format ("URL: %s ",url));
      System.out.println(String.format ("Method: %s ","PUT"));
      for( Header header: put.getAllHeaders()) {
       System.out.println(String.format("%s: %s",header.getName(),header.getValue()));
      }
    }

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    try {
      response = client.execute(put);
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
    return response;
  }

  private HttpResponse httpPut(String url,String apiKey ) throws Exception {

    HttpPut put = new HttpPut(url);
    put.addHeader(CONTINUUITY_API_KEY,apiKey);

    if (debugEnabled) {
      System.out.println(String.format ("Headers: %s ",put.getAllHeaders().toString()));
      System.out.println(String.format ("URL: %s ",url));
      System.out.println(String.format ("Method: %s ","PUT"));
      for( Header header: put.getAllHeaders()) {
        System.out.println(header.toString());
      }
    }

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    try {
      response = client.execute(put);
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
    return response;
  }

  private HttpResponse httpGet(String url,String apiKey ) throws Exception {

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


    return response;
  }


  private String getEndPoint(String hostname, String endpoint){
    return String.format("http://%s:7777/%s",hostname,endpoint);
  }

  public HttpResponse getVPCList(String hostname, String apiKey) throws Exception {
    String url  = getEndPoint(hostname,"passport/v1/vpc");
    return httpGet(url, apiKey);
  }

}
