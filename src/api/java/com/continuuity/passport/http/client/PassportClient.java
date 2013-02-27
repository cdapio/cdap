/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.client;

import com.continuuity.passport.PassportConstants;
import com.continuuity.passport.meta.Account;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.gson.*;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Client to communicate to the passport service.
 */

public class PassportClient {

  private boolean debugEnabled = false;
  private static Cache<String, String> responseCache = null;
  private static Cache<String, Account> accountCache = null;

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
   * @param baseURI
   * @return List of VPC Names
   * @throws Exception RunTimeExceptions
   */
  public List<String> getVPCList(URI baseURI,  String apiKey) throws RuntimeException {
    //Check in cache- if present return it.
    List<String> vpcList = Lists.newArrayList();

    try {
      URI uri = getEndPoint(baseURI, "passport/v1/vpc");

      String data = responseCache.getIfPresent(apiKey);
      if (data == null) {
        data = httpGet(uri, apiKey);

        if (data != null) {
          responseCache.put(apiKey, data);
        }
      }

      if (data != null) {
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(data);
        JsonArray jsonArray = element.getAsJsonArray();

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
   * @param baseURI uri of the service
   * @return Instance of {@AccountProvider}
   * @throws Exception RunTimeExceptions
   */
  public AccountProvider<Account> getAccount(URI baseURI, String apiKey) throws RuntimeException {
    Preconditions.checkNotNull(baseURI);

    try {
      URI uri = getEndPoint(baseURI, "passport/v1/account/authenticate");
      Account account = null;

      account = accountCache.getIfPresent(apiKey);
      if (account == null) {
        String data = httpPost(uri, apiKey);
        Gson gson  = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
        account = gson.fromJson(data, Account.class);
      }
      if(account != null) {
        accountCache.put(apiKey,account);
      }
      // This is a hack for overriding accountId type to String.
      // Ideally Account should use String type for account id instead.
      return new AccountProvider<Account>(account);
    }  catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }


  private String httpGet(URI uri, String apiKey) throws RuntimeException {
    String payload = null;
    HttpGet get = new HttpGet(uri);
    get.addHeader(PassportConstants.CONTINUUITY_API_KEY_HEADER, apiKey);

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    try {
      response = client.execute(get);
      payload = IOUtils.toString(response.getEntity().getContent());

      if(response.getStatusLine().getStatusCode() != 200){
        throw new RuntimeException(String.format("Call failed with status : %d",
          response.getStatusLine().getStatusCode()));
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
     finally {
      client.getConnectionManager().shutdown();
    }
    return payload;
  }

  private String httpPost(URI uri, String apiKey) throws RuntimeException {
    String payload = null;
    HttpPost post = new HttpPost(uri);
    post.addHeader(PassportConstants.CONTINUUITY_API_KEY_HEADER, apiKey);
    //Ad content type
    post.addHeader(PassportConstants.CONTENT_TYPE_HEADER,PassportConstants.CONTENT_TYPE);

    // prepare for HTTP
    HttpClient client = new DefaultHttpClient();

    HttpResponse response;

    try {
      response = client.execute(post);

      if(response.getStatusLine().getStatusCode() != 200){
        throw new RuntimeException(String.format("Call failed with status : %d",
          response.getStatusLine().getStatusCode()));
      }
      payload = IOUtils.toString(response.getEntity().getContent());

    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally{
      client.getConnectionManager().shutdown();
    }
    return payload;
  }

  private URI getEndPoint(URI baseURI, String endpoint) throws MalformedURLException, URISyntaxException {
    return new URI (baseURI.getScheme(),baseURI.getUserInfo(),baseURI.getHost(),
                    baseURI.getPort(),baseURI.getPath()+"/"+endpoint,baseURI.getQuery(),baseURI.getFragment());
  }
}
