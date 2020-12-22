/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import com.google.gson.Gson;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.proto.metadata.MetadataSearchResponse;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SearchClient for internal searching from metadata service
 */
public class MetadataSearchClient {

  private static final String QUERY_PARAM_FORMAT = "%s=%s";
  private static final String METADATA_SEARCH_BASE = "metadata/search";
  private static final String PARAM_DELIMITER = "&";
  private static final String PARAM_SEPARATOR = "?";
  private static final Gson GSON = new Gson();
  private final RemoteClient remoteClient;

  MetadataSearchClient(DiscoveryServiceClient discoveryClient) {
    this.remoteClient = new RemoteClient(discoveryClient, Constants.Service.METADATA_SERVICE,
                                         new DefaultHttpRequestConfig(false), Constants.Gateway.API_VERSION_3);
  }

  /**
   * Sends a HTTP GET request to metadata service with search params
   *
   * @param request {@link SearchRequest}
   * @return {@link MetadataSearchResponse}
   * @throws IOException
   */
  public MetadataSearchResponse search(SearchRequest request) throws IOException {
    HttpRequest httpRequest = remoteClient
      .requestBuilder(HttpMethod.GET, buildRequestURL(request)).build();
    HttpResponse response = remoteClient.execute(httpRequest);
    return GSON.fromJson(response.getResponseBodyAsString(), MetadataSearchResponse.class);
  }

  private String buildRequestURL(SearchRequest request) {
    StringBuilder builder = new StringBuilder();
    builder.append(METADATA_SEARCH_BASE);
    builder.append(PARAM_SEPARATOR);
    String simpleParamString = getSimpleParams(request).entrySet().stream()
      .filter(this::isPresent)
      .map(this::getFormattedParam)
      .collect(Collectors.joining(PARAM_DELIMITER));
    String multiParamsString = getMultiParams(request).entrySet().stream()
      .filter(this::isSetPresent)
      .map(this::getFormattedParamSet)
      .collect(Collectors.joining(PARAM_DELIMITER));
    builder.append(simpleParamString);
    builder.append(PARAM_DELIMITER);
    builder.append(multiParamsString);
    return builder.toString();
  }

  private boolean isPresent(Map.Entry<String, String> paramEntry) {
    return paramEntry.getValue() != null && !paramEntry.getValue().isEmpty();
  }

  private boolean isSetPresent(Map.Entry<String, Set<String>> paramEntry) {
    return paramEntry.getValue() != null && !paramEntry.getValue().isEmpty();
  }

  private String getFormattedParam(Map.Entry<String, String> paramEntry) {
    return getFormattedParam(paramEntry.getKey(), paramEntry.getValue());
  }

  private String getFormattedParam(String key, String value) {
    try {
      return String
        .format(QUERY_PARAM_FORMAT, key, URLEncoder.encode(value, StandardCharsets.UTF_8.name()));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private String getFormattedParamSet(Map.Entry<String, Set<String>> paramEntry) {
    return paramEntry.getValue().stream()
      .map(value -> getFormattedParam(paramEntry.getKey(), value))
      .collect(Collectors.joining(PARAM_DELIMITER));
  }

  private Map<String, String> getSimpleParams(SearchRequest request) {
    Map<String, String> params = new HashMap<>();
    params.put("query", request.getQuery());
    params.put("showHidden", String.valueOf(request.isShowHidden()));
    params.put("cursorRequested", String.valueOf(request.isCursorRequested()));
    params.put("offset", String.valueOf(request.getOffset()));
    params.put("limit", String.valueOf(request.getLimit()));
    params.put("scope", request.getScope().name());
    params.put("cursor", request.getCursor());
    if (request.getSorting() == null) {
      return params;
    }
    params.put("sort", request.getSorting().getKey());
    return params;
  }

  private Map<String, Set<String>> getMultiParams(SearchRequest request) {
    Map<String, Set<String>> params = new HashMap<>();
    params.put("namespaces", request.getNamespaces());
    if (request.getTypes() != null) {
      params.put("target", request.getTypes());
    }
    return params;
  }
}
