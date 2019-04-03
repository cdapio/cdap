/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.cli.command;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.Categorized;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.exception.CommandInputError;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.cli.util.FilePathResolver;
import io.cdap.cdap.client.ServiceClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.common.cli.Arguments;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpResponse;

import java.io.PrintStream;
import java.net.URL;
import java.util.Map;

/**
 * Call an endpoint of a {@link Service}.
 */
public class CallServiceCommand extends AbstractCommand implements Categorized {
  private static final Gson GSON = new Gson();


  private final ClientConfig clientConfig;
  private final RESTClient restClient;
  private final ServiceClient serviceClient;
  private final FilePathResolver filePathResolver;

  @Inject
  public CallServiceCommand(ClientConfig clientConfig, RESTClient restClient,
                            ServiceClient serviceClient, CLIConfig cliConfig,
                            FilePathResolver filePathResolver) {
    super(cliConfig);
    this.clientConfig = clientConfig;
    this.restClient = restClient;
    this.serviceClient = serviceClient;
    this.filePathResolver = filePathResolver;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {


    String method = arguments.get(ArgumentName.HTTP_METHOD.toString());
    String path = arguments.get(ArgumentName.ENDPOINT.toString());
    path = path.startsWith("/") ? path.substring(1) : path;
    String headers = arguments.getOptional(ArgumentName.HEADERS.toString(), "");
    String bodyString = arguments.getOptional(ArgumentName.HTTP_BODY.toString(), "");
    String bodyFile = arguments.getOptional(ArgumentName.LOCAL_FILE_PATH.toString(), "");
    Preconditions.checkNotNull(bodyString);
    Preconditions.checkNotNull(bodyFile);
    if (!bodyString.isEmpty() && !bodyFile.isEmpty()) {
      String message = String.format("Please provide either [body <%s>] or [body:file <%s>], " +
                                       "but not both", ArgumentName.HTTP_BODY.toString(),
                                     ArgumentName.LOCAL_FILE_PATH.toString());
      throw new CommandInputError(this, message);
    }

    Map<String, String> headerMap = GSON.fromJson(headers, new TypeToken<Map<String, String>>() { }.getType());
    ServiceId service = new ServiceId(parseProgramId(arguments, ElementType.SERVICE));
    URL url = arguments.hasArgument(ArgumentName.APP_VERSION.getName()) ?
      new URL(serviceClient.getVersionedServiceURL(service), path) :
      new URL(serviceClient.getServiceURL(service), path);

    HttpMethod httpMethod = HttpMethod.valueOf(method);
    HttpRequest.Builder builder = HttpRequest.builder(httpMethod, url).addHeaders(headerMap);
    if (httpMethod == HttpMethod.GET && (!bodyFile.isEmpty() || !bodyString.isEmpty())) {
      throw new UnsupportedOperationException("Sending body in a GET request is not supported");
    }

    if (!bodyFile.isEmpty()) {
      builder.withBody(filePathResolver.resolvePathToFile(bodyFile));
    } else if (!bodyString.isEmpty()) {
      builder.withBody(bodyString);
    }

    HttpResponse response = restClient.execute(builder.build(), clientConfig.getAccessToken());
    output.printf("< %s %s\n", response.getResponseCode(), response.getResponseMessage());
    for (Map.Entry<String, String> header : response.getHeaders().entries()) {
      output.printf("< %s: %s\n", header.getKey(), header.getValue());
    }
    output.print(response.getResponseBodyAsString());
  }

  @Override
  public String getPattern() {
    return String.format("call service <%s> [version <%s>] <%s> <%s> [headers <%s>] [body <%s>] [body:file <%s>]",
                         ArgumentName.SERVICE, ArgumentName.APP_VERSION, ArgumentName.HTTP_METHOD,
                         ArgumentName.ENDPOINT, ArgumentName.HEADERS, ArgumentName.HTTP_BODY,
                         ArgumentName.LOCAL_FILE_PATH);
  }

  @Override
  public String getDescription() {
    return String.format("Calls %s endpoint. The '<%s>' are formatted as '{\"key\":\"value\", ...}'. " +
                         "The request body may be provided as either a string or a file. " +
                         "To provide the body as a string, use 'body <%s>'. " +
                         "To provide the body as a file, use 'body:file <%s>'.",
                         Fragment.of(Article.A, ElementType.SERVICE.getName()),
                         ArgumentName.HEADERS, ArgumentName.HTTP_BODY, ArgumentName.LOCAL_FILE_PATH);
  }

  @Override
  public String getCategory() {
    return CommandCategory.EGRESS.getName();
  }
}
