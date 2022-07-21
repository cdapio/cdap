/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.WorkflowTokenNodeDetail;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.codec.WorkflowTokenDetailCodec;
import io.cdap.cdap.proto.codec.WorkflowTokenNodeDetailCodec;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;

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

  protected RemoteOpsClient(RemoteClientFactory remoteClientFactory, final String discoverableServiceName) {
    this.remoteClient = remoteClientFactory.createRemoteClient(discoverableServiceName,
                                                               new DefaultHttpRequestConfig(false),
                                                               "/v1/execute/");
  }

  protected HttpResponse executeRequest(String methodName, Object... arguments) throws UnauthorizedException {
    return executeRequest(methodName, ImmutableMap.<String, String>of(), arguments);
  }

  protected HttpResponse executeRequest(String methodName, Map<String, String> headers, Object... arguments)
    throws UnauthorizedException {
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
