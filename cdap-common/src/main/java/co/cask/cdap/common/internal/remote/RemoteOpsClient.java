/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.internal.remote;

import co.cask.cdap.common.ServiceUnavailableException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.codec.BasicThrowableCodec;
import co.cask.cdap.proto.codec.WorkflowTokenDetailCodec;
import co.cask.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Common HTTP client functionality for remote operations from programs.
 */
public class RemoteOpsClient {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec())
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
    .create();

  private final Supplier<EndpointStrategy> endpointStrategySupplier;
  private final HttpRequestConfig httpRequestConfig;
  private final String discoverableServiceName;

  protected RemoteOpsClient(CConfiguration cConf, final DiscoveryServiceClient discoveryClient,
                            final String discoverableServiceName) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(discoverableServiceName));
      }
    });

    this.httpRequestConfig = new DefaultHttpRequestConfig(false);
    this.discoverableServiceName = discoverableServiceName;
  }

  protected HttpResponse executeRequest(String methodName, Object... arguments) {
    return executeRequest(methodName, ImmutableMap.<String, String>of(), arguments);
  }

  protected HttpResponse executeRequest(String methodName, Map<String, String> headers, Object... arguments) {
    return doRequest("execute/" + methodName, HttpMethod.POST, headers, GSON.toJson(createArguments(arguments)));
  }

  private String resolve(String resource) {
    Discoverable discoverable = endpointStrategySupplier.get().pick();
    if (discoverable == null) {
      throw new ServiceUnavailableException(discoverableServiceName);
    }
    InetSocketAddress address = discoverable.getSocketAddress();
    String scheme = Arrays.equals(Constants.Security.SSL_URI_SCHEME.getBytes(), discoverable.getPayload()) ?
      Constants.Security.SSL_URI_SCHEME : Constants.Security.URI_SCHEME;
    return String.format("%s%s:%s%s/%s", scheme, address.getHostName(), address.getPort(), "/v1", resource);
  }

  private static List<MethodArgument> createArguments(Object... arguments) {
    List<MethodArgument> methodArguments = new ArrayList<>();
    for (Object arg : arguments) {
      if (arg == null) {
        methodArguments.add(null);
      } else {
        String type = arg.getClass().getName();
        methodArguments.add(new MethodArgument(type, GSON.toJsonTree(arg)));
      }
    }
    return methodArguments;
  }

  private HttpResponse doRequest(String resource, HttpMethod requestMethod,
                                 @Nullable Map<String, String> headers, @Nullable String body) {
    String resolvedUrl = resolve(resource);
    try {
      URL url = new URL(resolvedUrl);
      HttpRequest.Builder builder = HttpRequest.builder(requestMethod, url).addHeaders(headers);
      if (body != null) {
        builder.withBody(body);
      }
      HttpResponse response = HttpRequests.execute(builder.build(), httpRequestConfig);
      if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
        return response;
      }
      throw new RuntimeException(String.format("%s Response: %s.",
                                               createErrorMessage(resolvedUrl, requestMethod, headers, body),
                                               response));
    } catch (IOException e) {
      throw new RuntimeException(createErrorMessage(resolvedUrl, requestMethod, headers, body), e);
    }
  }

  // creates error message, encoding details about the request
  private String createErrorMessage(String resolvedUrl, HttpMethod requestMethod,
                                    @Nullable Map<String, String> headers, @Nullable String body) {
    return String.format("Error making request to %s service at %s while doing %s with headers %s and body %s.",
                         discoverableServiceName, resolvedUrl, requestMethod,
                         headers == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(headers),
                         body == null ? "null" : body);
  }
}
