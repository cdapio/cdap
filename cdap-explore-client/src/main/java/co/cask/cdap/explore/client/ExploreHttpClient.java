/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
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
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryHandle;
import co.cask.cdap.proto.QueryInfo;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.cdap.proto.TableInfo;
import co.cask.cdap.proto.TableNameInfo;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
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
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private static final Type MAP_TYPE_TOKEN = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type TABLES_TYPE = new TypeToken<List<TableNameInfo>>() { }.getType();
  private static final Type COL_DESC_LIST_TYPE = new TypeToken<List<ColumnDesc>>() { }.getType();
  private static final Type QUERY_INFO_LIST_TYPE = new TypeToken<List<QueryInfo>>() { }.getType();
  private static final Type ROW_LIST_TYPE = new TypeToken<List<QueryResult>>() { }.getType();

  protected abstract InetSocketAddress getExploreServiceAddress();

  protected abstract String getAuthToken();

  protected abstract boolean isSSLEnabled();

  protected abstract boolean verifySSLCert();

  protected boolean isAvailable() {
    try {
      HttpResponse response = doGet("explore/status");
      return response.getResponseCode() == HttpURLConnection.HTTP_OK;
    } catch (Exception e) {
      LOG.info("Caught exception when checking Explore availability", e);
      return false;
    }
  }

  protected QueryHandle doEnableExploreStream(Id.Stream stream, String tableName,
                                              FormatSpecification format) throws ExploreException {
    HttpResponse response = doPost(String.format(
      "namespaces/%s/data/explore/streams/%s/tables/%s/enable",
      stream.getNamespaceId(), stream.getId(), tableName), format == null ? null : GSON.toJson(format), null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException(String.format("Cannot enable explore on stream %s with table %s. Reason: %s",
                                             stream.getId(), tableName, response));
  }

  protected QueryHandle doDisableExploreStream(Id.Stream stream, String tableName) throws ExploreException {
    HttpResponse response = doPost(String.format("namespaces/%s/data/explore/streams/%s/tables/%s/disable",
                                                 stream.getNamespaceId(), stream.getId(), tableName), null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException(String.format("Cannot disable explore on stream %s with table %s. Reason: %s",
                                             stream.getId(), tableName, response));
  }

  protected QueryHandle doAddPartition(Id.DatasetInstance datasetInstance,
                                       PartitionKey key, String path) throws ExploreException {
    Map<String, String> args = Maps.newHashMap();
    PartitionedFileSetArguments.setOutputPartitionKey(args, key);
    args.put("path", path);
    HttpResponse response = doPost(String.format("namespaces/%s/data/explore/datasets/%s/partitions",
                                                 datasetInstance.getNamespaceId(), datasetInstance.getId()),
                                   GSON.toJson(args), null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException(String.format("Cannot add partition with key %s to dataset %s. Reason: %s",
                                             key, datasetInstance.toString(), response));
  }

  protected QueryHandle doDropPartition(Id.DatasetInstance datasetInstance, PartitionKey key) throws ExploreException {
    Map<String, String> args = Maps.newHashMap();
    PartitionedFileSetArguments.setOutputPartitionKey(args, key);
    HttpResponse response = doPost(String.format("namespaces/%s/data/explore/datasets/%s/deletePartition",
                                                 datasetInstance.getNamespaceId(), datasetInstance.getId()),
                                     GSON.toJson(args), null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException(String.format("Cannot drop partition with key %s from dataset %s. Reason: %s",
                                             key, datasetInstance.toString(), response));
  }

  protected QueryHandle doEnableExploreDataset(Id.DatasetInstance datasetInstance) throws ExploreException {
    HttpResponse response = doPost(String.format("namespaces/%s/data/explore/datasets/%s/enable",
                                                 datasetInstance.getNamespaceId(),
                                                 datasetInstance.getId()), null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException(String.format("Cannot enable explore on dataset %s. Reason: %s",
                                             datasetInstance.toString(), response));
  }

  protected QueryHandle doDisableExploreDataset(Id.DatasetInstance datasetInstance) throws ExploreException {
    HttpResponse response = doPost(String.format("namespaces/%s/data/explore/datasets/%s/disable",
                                                 datasetInstance.getNamespaceId(), datasetInstance.getId()),
                                   null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException(String.format("Cannot disable explore on dataset %s. Reason: %s",
                                             datasetInstance.toString(), response));
  }

  @Override
  public QueryHandle execute(Id.Namespace namespace, String statement) throws ExploreException {
    HttpResponse response = doPost(String.format("namespaces/%s/data/explore/queries", namespace.getId()),
                                   GSON.toJson(ImmutableMap.of("query", statement)), null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot execute query. Reason: " + response);
  }

  @Override
  public QueryStatus getStatus(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doGet(String.format("data/explore/queries/%s/%s",
                                                handle.getHandle(), "status"));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, QueryStatus.class);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot get status. Reason: " + response);
  }

  @Override
  public List<ColumnDesc> getResultSchema(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doGet(String.format("data/explore/queries/%s/%s",
                                                handle.getHandle(), "schema"));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, COL_DESC_LIST_TYPE);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot get result schema. Reason: " + response);
  }

  @Override
  public List<QueryResult> nextResults(QueryHandle handle, int size) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doPost(String.format("data/explore/queries/%s/%s",
                                                 handle.getHandle(), "next"),
                                   GSON.toJson(ImmutableMap.of("size", size)), null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, ROW_LIST_TYPE);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot get next results. Reason: " + response);
  }

  @Override
  public List<QueryResult> previewResults(QueryHandle handle)
    throws ExploreException, HandleNotFoundException, SQLException {
    HttpResponse response = doPost(String.format("data/explore/queries/%s/%s",
                                                 handle.getHandle(), "preview"),
                                   null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, ROW_LIST_TYPE);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot get results preview. Reason: " + response);
  }

  @Override
  public void close(QueryHandle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doDelete(String.format("data/explore/queries/%s", handle.getHandle()));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return;
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new HandleNotFoundException("Handle " + handle.getHandle() + "not found.");
    }
    throw new ExploreException("Cannot close operation. Reason: " + response);
  }

  @Override
  public int getActiveQueryCount(Id.Namespace namespace) throws ExploreException {
    String resource = String.format("namespaces/%s/data/explore/queries/count", namespace.getId());
    HttpResponse response = doGet(resource);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      Map<String, String> mapResponse = parseJson(response, new TypeToken<Map<String, String>>() { }.getType());
      return Integer.parseInt(mapResponse.get("count"));
    }
    throw new ExploreException("Cannot get list of queries. Reason: " + response);
  }

  @Override
  public List<QueryInfo> getQueries(Id.Namespace namespace) throws ExploreException, SQLException {
    String resource = String.format("namespaces/%s/data/explore/queries/", namespace.getId());
    HttpResponse response = doGet(resource);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, QUERY_INFO_LIST_TYPE);
    }
    throw new ExploreException("Cannot get list of queries. Reason: " + response);
  }

  @Override
  public QueryHandle getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    throws ExploreException, SQLException {
    String body = GSON.toJson(new ColumnsArgs(catalog, schemaPattern,
                                                                tableNamePattern, columnNamePattern));
    String resource = String.format("namespaces/%s/data/explore/jdbc/columns", schemaPattern);
    HttpResponse response = doPost(resource, body, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the columns. Reason: " + response);
  }

  @Override
  public QueryHandle getCatalogs() throws ExploreException, SQLException {
    HttpResponse response = doPost("data/explore/jdbc/catalogs", null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the catalogs. Reason: " + response);
  }

  @Override
  public QueryHandle getSchemas(String catalog, String schemaPattern) throws ExploreException, SQLException {
    String body = GSON.toJson(new SchemasArgs(catalog, schemaPattern));
    String resource = String.format("namespaces/%s/data/explore/jdbc/schemas", schemaPattern);
    HttpResponse response = doPost(resource, body, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the schemas. Reason: " + response);
  }

  @Override
  public QueryHandle getFunctions(String catalog, String schemaPattern, String functionNamePattern)
    throws ExploreException, SQLException {
    String body = GSON.toJson(new FunctionsArgs(catalog, schemaPattern, functionNamePattern));
    String resource = String.format("namespaces/%s/data/explore/jdbc/functions", schemaPattern);
    HttpResponse response = doPost(resource, body, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the functions. Reason: " + response);
  }

  @Override
  public MetaDataInfo getInfo(MetaDataInfo.InfoType infoType) throws ExploreException, SQLException {
    HttpResponse response = doGet(String.format("data/explore/jdbc/info/%s", infoType.name()));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, MetaDataInfo.class);
    }
    throw new ExploreException("Cannot get information " + infoType.name() + ". Reason: " + response);
  }

  @Override
  public QueryHandle getTables(String catalog, String schemaPattern,
                               String tableNamePattern, List<String> tableTypes) throws ExploreException, SQLException {
    String body = GSON.toJson(new TablesArgs(catalog, schemaPattern, tableNamePattern, tableTypes));
    String resource = String.format("namespaces/%s/data/explore/jdbc/tables", schemaPattern);
    HttpResponse response = doPost(resource, body, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the tables. Reason: " + response);
  }

  @Override
  public List<TableNameInfo> getTables(@Nullable String database) throws ExploreException {
    HttpResponse response = doGet(String.format("namespaces/%s/data/explore/tables", database));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, TABLES_TYPE);
    }
    throw new ExploreException("Cannot get the tables. Reason: " + response);
  }

  @Override
  public TableInfo getTableInfo(@Nullable String database, String table)
    throws ExploreException, TableNotFoundException {
    HttpResponse response = doGet(String.format("namespaces/%s/data/explore/tables/%s/info", database, table));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return parseJson(response, TableInfo.class);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TableNotFoundException("Table " + database + table + " not found.");
    }
    throw new ExploreException("Cannot get the schema of table " + database + table +
                               ". Reason: " + response);
  }

  @Override
  public QueryHandle getTableTypes() throws ExploreException, SQLException {
    HttpResponse response = doPost("data/explore/jdbc/tableTypes", null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the tables. Reason: " + response);
  }

  @Override
  public QueryHandle getTypeInfo() throws ExploreException, SQLException {
    HttpResponse response = doPost("data/explore/jdbc/types", null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot get the tables. Reason: " + response);
  }

  @Override
  public QueryHandle createNamespace(Id.Namespace namespace) throws ExploreException, SQLException {
    HttpResponse response = doPut(String.format("data/explore/namespaces/%s", namespace.getId()), null, null);
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot add a namespace. Reason: " + response);
  }

  @Override
  public QueryHandle deleteNamespace(Id.Namespace namespace) throws ExploreException, SQLException {
    HttpResponse response = doDelete(String.format("data/explore/namespaces/%s", namespace.getId()));
    if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
      return QueryHandle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot remove a namespace. Reason: " + response);
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
    String responseString = response.getResponseBodyAsString();
    try {
      return GSON.fromJson(responseString, type);
    } catch (JsonParseException e) {
      String message = String.format("Cannot parse server response: %s", responseString);
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
    if (getAuthToken() != null && !getAuthToken().isEmpty()) {
      newHeaders = (headers != null) ? Maps.newHashMap(headers) : Maps.<String, String>newHashMap();
      newHeaders.put("Authorization", "Bearer " + getAuthToken());
    }
    String resolvedUrl = resolve(resource);
    try {
      URL url = new URL(resolvedUrl);
      HttpRequest.Builder builder = HttpRequest.builder(HttpMethod.valueOf(requestMethod), url).addHeaders(newHeaders);
      if (body != null) {
        builder.withBody(body);
      }
      return HttpRequests.execute(builder.build(), createRequestConfig());
    } catch (IOException e) {
      throw new ExploreException(
        String.format("Error connecting to Explore Service at %s while doing %s with headers %s and body %s",
                      resolvedUrl, requestMethod,
                      newHeaders == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(newHeaders),
                      body == null ? "null" : body), e);
    }
  }

  private HttpRequestConfig createRequestConfig() {
    return new HttpRequestConfig(HttpRequestConfig.DEFAULT.getConnectTimeout(),
                                 HttpRequestConfig.DEFAULT.getReadTimeout(),
                                 verifySSLCert());
  }

  private String resolve(String resource) {
    InetSocketAddress addr = getExploreServiceAddress();
    String url = String.format("%s://%s:%s%s/%s", isSSLEnabled() ? "https" : "http",
                               addr.getHostName(), addr.getPort(),
                               Constants.Gateway.API_VERSION_3, resource);
    LOG.trace("Explore URL = {}", url);
    return url;
  }

}
