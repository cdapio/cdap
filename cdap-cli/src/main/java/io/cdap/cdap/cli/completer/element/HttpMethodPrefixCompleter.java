/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.cli.completer.element;

import com.google.common.collect.Lists;
import io.cdap.cdap.api.service.http.ServiceHttpEndpoint;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ProgramIdArgument;
import io.cdap.cdap.cli.util.ArgumentParser;
import io.cdap.cdap.client.ServiceClient;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.cli.completers.PrefixCompleter;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Prefix completer for Http methods.
 */
public class HttpMethodPrefixCompleter extends PrefixCompleter {

  private static final String SERVICE_ID = "app-id.service-id";
  private static final String APP_VERSION = "app-version";
  private static final String PATTERN = String.format("call service <%s> [version <%s>]", SERVICE_ID, APP_VERSION);

  private final ServiceClient serviceClient;
  private final EndpointCompleter completer;
  private final CLIConfig cliConfig;

  public HttpMethodPrefixCompleter(final ServiceClient serviceClient, final CLIConfig cliConfig,
                                   String prefix, EndpointCompleter completer) {
    super(prefix, completer);
    this.cliConfig = cliConfig;
    this.serviceClient = serviceClient;
    this.completer = completer;
  }

  @Override
  public int complete(String buffer, int cursor, List<CharSequence> candidates) {
    Map<String, String> arguments = ArgumentParser.getArguments(buffer, PATTERN);
    ProgramIdArgument programIdArgument = ArgumentParser.parseProgramId(arguments.get(SERVICE_ID));
    if (programIdArgument != null) {
      ServiceId service;
      if (arguments.get(APP_VERSION) == null) {
        service = cliConfig.getCurrentNamespace().app(programIdArgument.getAppId()).service(
          programIdArgument.getProgramId());
      } else {
        service = cliConfig.getCurrentNamespace().app(programIdArgument.getAppId(), arguments.get(APP_VERSION)).service(
          programIdArgument.getProgramId());
      }
      completer.setEndpoints(getMethods(service));
    } else {
      completer.setEndpoints(Collections.<String>emptyList());
    }
    return super.complete(buffer, cursor, candidates);
  }

  public Collection<String> getMethods(ServiceId serviceId) {
    Collection<String> httpMethods = Lists.newArrayList();
    try {
      for (ServiceHttpEndpoint endpoint : serviceClient.getEndpoints(serviceId)) {
        String method = endpoint.getMethod();
        if (!httpMethods.contains(method)) {
          httpMethods.add(method);
        }
      }
    } catch (IOException | UnauthenticatedException | NotFoundException | UnauthorizedException ignored) {
    }
    return httpMethods;
  }
}
