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
import co.cask.cdap.api.service.http.ServiceHttpEndpoint;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AsciiTable;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.client.ServiceClient;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Get a list of the endpoints that a {@link Service} exposes.
 */
public class GetServiceEndpointsCommand implements Command {

  private final ServiceClient serviceClient;

  @Inject
  public GetServiceEndpointsCommand(ServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String[] appAndServiceId = arguments.get(ArgumentName.SERVICE.toString()).split("\\.");
    if (appAndServiceId.length < 2) {
      throw new CommandInputError(this);
    }

    String appId = appAndServiceId[0];
    String serviceId = appAndServiceId[1];
    List<ServiceHttpEndpoint> endpoints = serviceClient.getEndpoints(appId, serviceId);

    new AsciiTable<ServiceHttpEndpoint>(
      new String[] { "method", "path"},
      endpoints,
      new RowMaker<ServiceHttpEndpoint>() {
        @Override
        public Object[] makeRow(ServiceHttpEndpoint endpoint) {
          return new Object[] {
            endpoint.getMethod(),
            endpoint.getPath()
          };
        }
      }
    ).print(output);
  }

  @Override
  public String getPattern() {
    return String.format("get endpoints service <%s>", ArgumentName.SERVICE);
  }

  @Override
  public String getDescription() {
    return String.format("List the endpoints that a %s exposes", ElementType.SERVICE.getPrettyName());
  }
}
