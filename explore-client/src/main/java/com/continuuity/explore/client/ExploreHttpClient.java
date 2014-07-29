/*
 * Copyright 2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.explore.client;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.HttpMethod;
import com.continuuity.common.http.HttpRequest;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.MetaDataInfo;
import com.continuuity.explore.utils.ColumnsArgs;
import com.continuuity.explore.utils.FunctionsArgs;
import com.continuuity.explore.utils.SchemasArgs;
import com.continuuity.explore.utils.TablesArgs;
import com.continuuity.proto.ColumnDesc;
import com.continuuity.proto.QueryHandle;
import com.continuuity.proto.QueryResult;
import com.continuuity.proto.QueryStatus;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URL;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The methods of this class call the HTTP APIs exposed by explore and return the raw information
 * contained in their json responses. This class is only meant to be extended by classes
 * which implement ExploreClient.
 */
abstract class ExploreHttpClient implements Explore {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreHttpClient.class);
  private static final Gson GSON = new Gson();

  private static final Type MAP_TYPE_TOKEN = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type COL_DESC_LIST_TYPE = new TypeToken<List<ColumnDesc>>() { }.getType();
  private static final Type ROW_LIST_TYPE = new TypeToken<List<QueryResult>>() { }.getType();

  protected abstract InetSocketAddress getExploreServiceAddress();

  protected abstract String getAuthorizationToken();

  protected boolean isAvailable() {
    try {
      HttpResponse response = doGet("explore/status");
      return HttpResponseStatus.OK.getCode() == response.getResponseCode();
    } catch (Exception e) {
      LOG.info("Caught exception when checking Explore availability", e);
      return false;
    }
  }

  protected QueryHandle doEnableExplore(String datasetInstance) throws ExploreException {
    HttpResponse response = doPost(String.format("explore/instances/%s/enable", datasetInstance), null, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot enable explore on dataset " + datasetInstance + ". Reason: " +
                                 getDetails(response));
  }

  protected QueryHandle doDisableExplore(String datasetInstance) throws ExploreException {
    HttpResponse response = doPost(String.format("explore/instances/%s/disable", datasetInstance), null, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot disable explore on dataset " + datasetInstance + ". Reason: " +
                                 getDetails(response));
  }

  public Map<String, String> getDatasetSchema(String datasetName) throws ExploreException {
    HttpResponse response = doGet(String.format("data/explore/datasets/%s/schema", datasetName));
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return parseJson(response, MAP_TYPE_TOKEN);
    }
    throw new ExploreException("Cannot get dataset schema. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle execute(String statement) throws ExploreException {
    HttpResponse response = doPost("data/queries", GSON.toJson(ImmutableMap.of("query", statement)), null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot execute query. Reason: " + getDetails(response));
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doGet(String.format("data/queries/%s/%s", handle.getHandle(), "status"));
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return parseJson(response, QueryStatus.class);
    }
    throw new ExploreException("Cannot get status. Reason: " + getDetails(response));
  }

  @Override
  public List<ColumnDesc> getResultSchema(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doGet(String.format("data/queries/%s/%s", handle.getHandle(), "schema"));
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return parseJson(response, COL_DESC_LIST_TYPE);
    }
    throw new ExploreException("Cannot get result schema. Reason: " + getDetails(response));
  }

  @Override
  public List<QueryResult> nextResults(QueryHandle handle, int size) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doPost(String.format("data/queries/%s/%s", handle.getHandle(), "next"),
                                   GSON.toJson(ImmutableMap.of("size", size)), null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return parseJson(response, ROW_LIST_TYPE);
    }
    throw new ExploreException("Cannot get next results. Reason: " + getDetails(response));
  }

  @Override
  public void cancel(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doPost(String.format("data/queries/%s/%s", handle.getHandle(), "cancel"), null, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return;
    }
    throw new ExploreException("Cannot cancel operation. Reason: " + getDetails(response));
  }

  @Override
  public void close(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doDelete(String.format("data/queries/%s", handle.getHandle()));
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return;
    }
    throw new ExploreException("Cannot close operation. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    throws ExploreException, SQLException {
    String body = GSON.toJson(new ColumnsArgs(catalog, schemaPattern,
                                                                tableNamePattern, columnNamePattern));
    HttpResponse response = doPost("data/explore/jdbc/columns", body, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the columns. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getCatalogs() throws ExploreException, SQLException {
    HttpResponse response = doPost("data/explore/jdbc/catalogs", null, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the catalogs. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getSchemas(String catalog, String schemaPattern) throws ExploreException, SQLException {
    String body = GSON.toJson(new SchemasArgs(catalog, schemaPattern));
    HttpResponse response = doPost("data/explore/jdbc/schemas", body, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the schemas. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getFunctions(String catalog, String schemaPattern, String functionNamePattern)
    throws ExploreException, SQLException {
    String body = GSON.toJson(new FunctionsArgs(catalog, schemaPattern, functionNamePattern));
    HttpResponse response = doPost("data/explore/jdbc/functions", body, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the functions. Reason: " + getDetails(response));
  }

  @Override
  public MetaDataInfo getInfo(MetaDataInfo.InfoType infoType) throws ExploreException, SQLException {
    HttpResponse response = doGet(String.format("data/explore/jdbc/info/%s", infoType.name()));
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return parseJson(response, MetaDataInfo.class);
    }
    throw new ExploreException("Cannot get information " + infoType.name() + ". Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getTables(String catalog, String schemaPattern,
                          String tableNamePattern, List<String> tableTypes) throws ExploreException, SQLException {
    String body = GSON.toJson(new TablesArgs(catalog, schemaPattern,
                                                               tableNamePattern, tableTypes));
    HttpResponse response = doPost("data/explore/jdbc/tables", body, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the tables. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getTableTypes() throws ExploreException, SQLException {
    HttpResponse response = doPost("data/explore/jdbc/tableTypes", null, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the tables. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getTypeInfo() throws ExploreException, SQLException {
    HttpResponse response = doPost("data/explore/jdbc/types", null, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the tables. Reason: " + getDetails(response));
  }

  private String parseResponseAsMap(HttpResponse response, String key) throws ExploreException {
    Map<String, String> responseMap = parseJson(response, MAP_TYPE_TOKEN);
    if (responseMap.containsKey(key)) {
      return responseMap.get(key);
    }

    String message = String.format("Cannot find key %s in server response: %s", key,
                                   new String(response.getResponseBody(), Charsets.UTF_8));
    LOG.error(message);
    throw new ExploreException(message);
  }

  private <T> T parseJson(HttpResponse response, Type type) throws ExploreException {
    String responseString = new String(response.getResponseBody(), Charsets.UTF_8);
    try {
      return GSON.fromJson(responseString, type);
    } catch (JsonSyntaxException e) {
      String message = String.format("Cannot parse server response: %s", responseString);
      LOG.error(message, e);
      throw new ExploreException(message, e);
    } catch (JsonParseException e) {
      String message = String.format("Cannot parse server response as map: %s", responseString);
      LOG.error(message, e);
      throw new ExploreException(message, e);
    }
  }

  private HttpResponse doGet(String resource) throws ExploreException {
    return doRequest(resource, "GET", null, null);
  }

  private HttpResponse doPost(String resource, String body, Map<String, String> headers) throws ExploreException {
    return doRequest(resource, "POST", headers, body);
  }

  private HttpResponse doDelete(String resource) throws ExploreException {
    return doRequest(resource, "DELETE", null, null);
  }

  private HttpResponse doRequest(String resource, String requestMethod,
                                 @Nullable Map<String, String> headers,
                                 @Nullable String body) throws ExploreException {
    Map<String, String> newHeaders = headers;
    if (getAuthorizationToken() != null && !getAuthorizationToken().isEmpty()) {
      newHeaders = (headers != null) ? Maps.newHashMap(headers) : Maps.<String, String>newHashMap();
      newHeaders.put("Authorization", "Bearer " + getAuthorizationToken());
    }
    String resolvedUrl = resolve(resource);
    try {
      URL url = new URL(resolvedUrl);
      if (body != null) {
        return HttpRequests.execute(HttpRequest.builder(HttpMethod.valueOf(requestMethod), url)
                                      .addHeaders(newHeaders).withBody(body).build());
      } else {
        return HttpRequests.execute(HttpRequest.builder(HttpMethod.valueOf(requestMethod), url)
                                      .addHeaders(newHeaders).build());
      }
    } catch (IOException e) {
      throw new ExploreException(
        String.format("Error connecting to Explore Service at %s while doing %s with headers %s and body %s",
                      resolvedUrl, requestMethod,
                      newHeaders == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(newHeaders),
                      body == null ? "null" : body), e);
    }
  }

  private String getDetails(HttpResponse response) {
    return String.format("Response code: %s, message:'%s', body: '%s'",
                         response.getResponseCode(), response.getResponseMessage(),
                         response.getResponseBody() == null ?
                           "null" : new String(response.getResponseBody(), Charsets.UTF_8));

  }

  private String resolve(String resource) {
    InetSocketAddress addr = getExploreServiceAddress();
    String url = String.format("http://%s:%s%s/%s", addr.getHostName(), addr.getPort(),
                               Constants.Gateway.GATEWAY_VERSION, resource);
    LOG.trace("Explore URL = {}", url);
    return url;
  }

}
