/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.client;

import com.continuuity.common.conf.Constants;
import com.continuuity.passport.meta.Account;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Client to communicate to the passport service.
 */

public class PassportClient {
  private static final Logger LOG = LoggerFactory.getLogger(PassportClient.class);
  private static Cache<String, String> responseCache = null;
  private static Cache<String, Account> accountCache = null;
  private final URI baseUri;
  private static final String API_BASE = "/api/";

  public PassportClient() {
    this(URI.create("http://localhost"));
  }

  private PassportClient(URI baseUri) {
    Preconditions.checkNotNull(baseUri);
    this.baseUri = baseUri;
    //Cache valid responses from Servers for 10 mins
    responseCache = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .build();
    accountCache = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .build();
  }

  public static PassportClient create(String uri) {
    Preconditions.checkNotNull(uri);
    return new PassportClient(URI.create(uri));
  }

  /**
   * Get List of VPC for the apiKey.
   * @return List of VPC Names
   */
  public List<String> getVPCList(String apiKey) {
    Preconditions.checkNotNull(apiKey, "ApiKey cannot be null");
    List<String> vpcList = Lists.newArrayList();

    try {
      String data = responseCache.getIfPresent(apiKey);

      if (data == null) {
        data = httpGet(API_BASE + "vpc/list", apiKey);
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
      throw Throwables.propagate(e);
    }
    return vpcList;
  }


  /**
   * Get List of VPC for the apiKey.
   * @return Instance of {@code AccountProvider}
   * */
  public AccountProvider<Account> getAccount(String apiKey) {
    Preconditions.checkNotNull(apiKey, "ApiKey cannot be null");

    try {
      Account account = accountCache.getIfPresent(apiKey);
      if (account == null) {
        String data = httpGet(API_BASE + "whois", apiKey);
        if (data != null) {
          Gson gson  = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
          account = gson.fromJson(data, Account.class);
          accountCache.put(apiKey, account);
          return new AccountProvider<Account>(account);
        }
      } else {
        return new AccountProvider<Account>(account);
      }
      // This is a hack for overriding accountId type to String.
      // Ideally Account should use String type for account id instead
      return new AccountProvider<Account>(null);
    }  catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  private String httpGet(String api, String apiKey)  {
    URI uri = URI.create(String.format("%s/%s/%s", baseUri.toASCIIString(), api, apiKey));
    HttpGet get = new HttpGet(uri);
    get.addHeader(Constants.Gateway.CONTINUUITY_API_KEY, apiKey);
    return request(get);
  }

  private String request(HttpUriRequest uri)  {
    LOG.trace("Requesting " + uri.getURI().toASCIIString());
    HttpClient client = new DefaultHttpClient();
    try {
      HttpResponse response = client.execute(uri);
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new RuntimeException(String.format("Call failed with status : %d",
          response.getStatusLine().getStatusCode()));
      }
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ByteStreams.copy(response.getEntity().getContent(), bos);
      return bos.toString("UTF-8");
    } catch (IOException e) {
      LOG.warn("Failed to retrieve data from " + uri.getURI().toASCIIString(), e);
      return null;
    } finally {
      client.getConnectionManager().shutdown();
    }
  }
}
