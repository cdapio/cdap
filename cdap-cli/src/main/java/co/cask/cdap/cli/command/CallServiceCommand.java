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
 * the License
 */

package co.cask.cdap.cli.command;

import co.cask.cdap.api.service.Service;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AsciiTable;
import co.cask.cdap.cli.util.ResponseUtil;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.StringUtils;
import co.cask.cdap.common.http.HttpMethod;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Call an endpoint of a {@link Service}.
 */
public class CallServiceCommand implements Command {
  private static final Gson GSON = new Gson();


  private final ClientConfig clientConfig;
  private final RESTClient restClient;
  private final ServiceClient serviceClient;

  @Inject
  public CallServiceCommand(ClientConfig clientConfig, ServiceClient serviceClient) {
    this.clientConfig = clientConfig;
    this.restClient = RESTClient.create(clientConfig);
    this.serviceClient = serviceClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String[] appAndServiceId = arguments.get(ArgumentName.SERVICE.toString()).split("\\.");
    String appId = appAndServiceId[0];
    String serviceId = appAndServiceId[1];
    String method = arguments.get(ArgumentName.HTTP_METHOD.toString());
    String path = arguments.get(ArgumentName.ENDPOINT.toString());
    path = path.startsWith("/") ? path.substring(1) : path;
    String headers = arguments.get(ArgumentName.HEADERS.toString(), "");
    String body = arguments.get(ArgumentName.HTTP_BODY.toString(), "");

    Map<String, String> headerMap = GSON.fromJson(headers, new TypeToken<Map<String, String>>() {
    }.getType());
    URL url = new URL(serviceClient.getServiceURL(appId, serviceId), path);

    HttpMethod httpMethod = HttpMethod.valueOf(method);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url).addHeaders(headerMap);
    if (!body.isEmpty() && httpMethod != HttpMethod.GET) {
      builder.withBody(body);
    }
    HttpResponse response = restClient.execute(builder.build(), clientConfig.getAccessToken());

    new AsciiTable<HttpResponse>(
      new String[] { "status", "headers", "body size", "body"},
      ImmutableList.of(response),
      new RowMaker<HttpResponse>() {
        @Override
        public Object[] makeRow(HttpResponse httpResponse) {
          ByteBuffer byteBuffer = ByteBuffer.wrap(httpResponse.getResponseBody());
          long bodySize = byteBuffer.remaining();

          return new Object[] {
            httpResponse.getResponseCode(),
            formatHeaders(httpResponse),
            bodySize,
            ResponseUtil.getBody(byteBuffer)
          };
        }
      }
    ).print(output);
  }

  @Override
  public String getPattern() {
    return String.format("call service <%s> <%s> <%s> [headers <%s>] [body <%s>]",
                         ArgumentName.SERVICE, ArgumentName.HTTP_METHOD,
                         ArgumentName.ENDPOINT, ArgumentName.HEADERS, ArgumentName.HTTP_BODY);
  }

  @Override
  public String getDescription() {
    return "Calls a " + ElementType.SERVICE.getPrettyName() + "endpoint." +
           "The header is formatted as \"{'key' : 'value', ...}\" and the body is a String.";
  }

  /**
   * Format multiple header values as a comma separated list of the values.
   * This is valid, as per: http://www.w3.org/Protocols/rfc2616/rfc2616-sec4.html
   */
  private String formatHeaders(HttpResponse response) {
    Map<String, List<String>> headers = response.getHeaders();
    ImmutableMap.Builder builder = new ImmutableMap.Builder();
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      if (entry.getKey() != null) {
        List<String> value = entry.getValue();
        builder.put(entry.getKey(), StringUtils.arrayToString(value.toArray(new String[value.size()])));
      }
    }
    return ResponseUtil.formatHeader(builder.build());
  }
}
