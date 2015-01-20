/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.client;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.explore.service.Explore;
import co.cask.cdap.explore.service.ExploreException;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.explore.service.MetaDataInfo;
import co.cask.cdap.explore.service.TableNotFoundException;
import co.cask.cdap.explore.utils.ColumnsArgs;
import co.cask.cdap.explore.utils.FunctionsArgs;
import co.cask.cdap.explore.utils.SchemasArgs;
import co.cask.cdap.explore.utils.TablesArgs;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryInfo;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.TableInfo;
import co.cask.cdap.proto.TableNameInfo;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
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
  private static final Type TABLES_TYPE = new TypeToken<List<TableNameInfo>>() { }.getType();
  private static final Type COL_DESC_LIST_TYPE = new TypeToken<List<ColumnDesc>>() { }.getType();
  private static final Type QUERY_INFO_LIST_TYPE = new TypeToken<List<QueryInfo>>() { }.getType();
  private static final Type ROW_LIST_TYPE = new TypeToken<List<QueryResult>>() { }.getType();

  protected abstract InetSocketAddress getExploreServiceAddress();

  protected abstract String getAuthorizationToken();

  protected boolean isAvailable() {
    try {
      HttpResponse response = doGet("explore/status");
      return response.getResponseCode() == HttpURLConnection.HTTP_OK;
    } catch (Exception e) {
      LOG.info("Caught exception when checking Explore availability", e);
      return false;
    }
  }

  protected QueryHandle doEnableExploreStream(String streamName) throws ExploreException {
    HttpResponse response = doPost(String.format("data/explore/streams/%s/enable", streamName), null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot enable explore on stream " + streamName + ". Reason: " +
                                 getDetails(response));
  }

  protected QueryHandle doDisableExploreStream(String streamName) throws ExploreException {
    HttpResponse response = doPost(String.format("data/explore/streams/%s/disable", streamName), null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot disable explore on stream " + streamName + ". Reason: " +
                                 getDetails(response));
  }

  protected QueryHandle doAddPartition(String datasetName, long time, String path) throws ExploreException {
    HttpResponse response = doPut(String.format("data/explore/datasets/%s/partitions/%d", datasetName, time),
                                  GSON.toJson(ImmutableMap.of("path", path)), null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot add partition with time " + time + "to dataset " + datasetName +
                                 ". Reason: " + getDetails(response));
  }

  protected QueryHandle doEnableExploreDataset(String datasetInstance) throws ExploreException {
    HttpResponse response = doPost(String.format("data/explore/datasets/%s/enable", datasetInstance), null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot enable explore on dataset " + datasetInstance + ". Reason: " +
                                 getDetails(response));
  }

  protected QueryHandle doDisableExploreDataset(String datasetInstance) throws ExploreException {
    HttpResponse response = doPost(String.format("data/explore/datasets/%s/disable", datasetInstance), null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot disable explore on dataset " + datasetInstance + ". Reason: " +
                                 getDetails(response));
  }

  @Override
  public QueryHandle execute(String statement) throws ExploreException {
    HttpResponse response = doPost("data/explore/queries", GSON.toJson(ImmutableMap.of("query", statement)), null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot execute query. Reason: " + getDetails(response));
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doGet(String.format("data/explore/queries/%s/%s", handle.getHandle(), "status"));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, QueryStatus.class);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot get status. Reason: " + getDetails(response));
  }

  @Override
  public List<ColumnDesc> getResultSchema(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doGet(String.format("data/explore/queries/%s/%s", handle.getHandle(), "schema"));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, COL_DESC_LIST_TYPE);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot get result schema. Reason: " + getDetails(response));
  }

  @Override
  public List<QueryResult> nextResults(QueryHandle handle, int size) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doPost(String.format("data/explore/queries/%s/%s", handle.getHandle(), "next"),
                                   GSON.toJson(ImmutableMap.of("size", size)), null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, ROW_LIST_TYPE);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot get next results. Reason: " + getDetails(response));
  }

  @Override
  public List<QueryResult> previewResults(QueryHandle handle)
    throws ExploreException, HandleNotFoundException, SQLException {
    HttpResponse response = doPost(String.format("data/explore/queries/%s/%s", handle.getHandle(), "preview"),
                                   null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, ROW_LIST_TYPE);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot get results preview. Reason: " + getDetails(response));
  }

  @Override
  public void close(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doDelete(String.format("data/explore/queries/%s", handle.getHandle()));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot close operation. Reason: " + getDetails(response));
  }

  @Override
  public List<QueryInfo> getQueries() throws ExploreException, SQLException {

    HttpResponse response = doGet("data/explore/queries/");
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, QUERY_INFO_LIST_TYPE);
    }
    throw new ExploreException("Cannot get list of queries. Reason: " + getDetails(response));
  }

  public QueryHandle getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    throws ExploreException, SQLException {
    String body = GSON.toJson(new ColumnsArgs(catalog, schemaPattern,
                                                                tableNamePattern, columnNamePattern));
    HttpResponse response = doPost("data/explore/jdbc/columns", body, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the columns. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getCatalogs() throws ExploreException, SQLException {
    HttpResponse response = doPost("data/explore/jdbc/catalogs", null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the catalogs. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getSchemas(String catalog, String schemaPattern) throws ExploreException, SQLException {
    String body = GSON.toJson(new SchemasArgs(catalog, schemaPattern));
    HttpResponse response = doPost("data/explore/jdbc/schemas", body, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the schemas. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getFunctions(String catalog, String schemaPattern, String functionNamePattern)
    throws ExploreException, SQLException {
    String body = GSON.toJson(new FunctionsArgs(catalog, schemaPattern, functionNamePattern));
    HttpResponse response = doPost("data/explore/jdbc/functions", body, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the functions. Reason: " + getDetails(response));
  }

  @Override
  public MetaDataInfo getInfo(MetaDataInfo.InfoType infoType) throws ExploreException, SQLException {
    HttpResponse response = doGet(String.format("data/explore/jdbc/info/%s", infoType.name()));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, MetaDataInfo.class);
    }
    throw new ExploreException("Cannot get information " + infoType.name() + ". Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getTables(String catalog, String schemaPattern,
                               String tableNamePattern, List<String> tableTypes) throws ExploreException, SQLException {
    String body = GSON.toJson(new TablesArgs(catalog, schemaPattern, tableNamePattern, tableTypes));
    HttpResponse response = doPost("data/explore/jdbc/tables", body, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the tables. Reason: " + getDetails(response));
  }

  @Override
  public List<TableNameInfo> getTables(@Nullable String database) throws ExploreException {
    HttpResponse response = doGet(String.format("data/explore/tables%s", (database != null) ? "?db=" + database : ""));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, TABLES_TYPE);
    }
    throw new ExploreException("Cannot get the tables. Reason: " + getDetails(response));
  }

  @Override
  public TableInfo getTableInfo(@Nullable String database, String table)
    throws ExploreException, TableNotFoundException {
    String tableNamePrefix = (database != null) ? database + "." : "";
    HttpResponse response = doGet(String.format("data/explore/tables/%s/info", tableNamePrefix + table));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, TableInfo.class);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TableNotFoundException("Table " + tableNamePrefix + table + " not found.");
    }
    throw new ExploreException("Cannot get the schema of table " + tableNamePrefix + table +
                               ". Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getTableTypes() throws ExploreException, SQLException {
    HttpResponse response = doPost("data/explore/jdbc/tableTypes", null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the tables. Reason: " + getDetails(response));
  }

  @Override
  public QueryHandle getTypeInfo() throws ExploreException, SQLException {
    HttpResponse response = doPost("data/explore/jdbc/types", null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
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

  private HttpResponse doPut(String resource, String body, Map<String, String> headers) throws ExploreException {
    return doRequest(resource, "PUT", headers, body);
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
                               Constants.Gateway.API_VERSION_2, resource);
    LOG.trace("Explore URL = {}", url);
    return url;
  }

}
