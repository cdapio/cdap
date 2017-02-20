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

import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.codec.BasicThrowableCodec;
import co.cask.cdap.proto.codec.WorkflowTokenDetailCodec;
import co.cask.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import co.cask.common.http.HttpMethod;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Common HTTP client functionality for remote operations from programs.
 */
public class RemoteOpsClient {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec())
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .registerTypeAdapter(WorkflowTokenNodeDetail.class, new WorkflowTokenNodeDetailCodec())
    .create();

  private final RemoteClient remoteClient;

  protected RemoteOpsClient(final DiscoveryServiceClient discoveryClient,
                            final String discoverableServiceName) {
    this.remoteClient = new RemoteClient(discoveryClient, discoverableServiceName,
                                         new DefaultHttpRequestConfig(false), "/v1/execute/");
  }

  protected HttpResponse executeRequest(String methodName, Object... arguments) {
    return executeRequest(methodName, ImmutableMap.<String, String>of(), arguments);
  }

  protected HttpResponse executeRequest(String methodName, Map<String, String> headers, Object... arguments) {
    String body = GSON.toJson(createBody(arguments));
    HttpRequest.Builder builder = remoteClient.requestBuilder(HttpMethod.POST, methodName).addHeaders(headers);
    if (body != null) {
      builder.withBody(body);
    }
    HttpRequest request = builder.build();
    try {
      HttpResponse response = remoteClient.execute(request);
      if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
        return response;
      }
      throw new RuntimeException(String.format("%s Response: %s.",
                                               remoteClient.createErrorMessage(request, body),
                                               response));
    } catch (IOException e) {
      throw new RuntimeException(remoteClient.createErrorMessage(request, body), e);
    }
  }

  private static List<MethodArgument> createBody(Object... arguments) {
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

}
