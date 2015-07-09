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

package co.cask.cdap.cli.completer.element;

import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.cli.ProgramIdArgument;
import co.cask.cdap.cli.util.ArgumentParser;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.common.cli.completers.PrefixCompleter;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Prefix completer for Http endpoints.
 */
public class HttpEndpointPrefixCompleter extends PrefixCompleter {

  private static final String PROGRAM_ID = "programId";
  private static final String METHOD = "method";
  private static final String PATTERN = String.format("call service <%s> <%s>", PROGRAM_ID, METHOD);

  private final ServiceClient serviceClient;
  private final EndpointCompleter completer;

  public HttpEndpointPrefixCompleter(final ServiceClient serviceClient, String prefix, EndpointCompleter completer) {
    super(prefix, completer);
    this.serviceClient = serviceClient;
    this.completer = completer;
  }

  @Override
  public int complete(String buffer, int cursor, List<CharSequence> candidates) {
    Map<String, String> arguments = ArgumentParser.getArguments(buffer, PATTERN);
    ProgramIdArgument programIdArgument = ArgumentParser.parseProgramId(arguments.get(PROGRAM_ID));
    if (programIdArgument != null) {
      completer.setEndpoints(getEndpoints(programIdArgument.getAppId(), programIdArgument.getProgramId(),
                                          arguments.get(METHOD)));
    } else {
      completer.setEndpoints(Collections.<String>emptyList());
    }
    return super.complete(buffer, cursor, candidates);
  }

  public Collection<String> getEndpoints(String appId, String serviceId, String method) {
    Collection<String> httpEndpoints = Lists.newArrayList();
    try {
      for (ServiceHttpEndpoint endpoint : serviceClient.getEndpoints(appId, serviceId)) {
        if (endpoint.getMethod().equals(method)) {
          httpEndpoints.add(endpoint.getPath());
        }
      }
    } catch (IOException ignored) {
    } catch (UnauthorizedException ignored) {
    } catch (NotFoundException ignored) {
    }
    return httpEndpoints;
  }
}
