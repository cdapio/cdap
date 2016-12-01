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
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * Get a list of the endpoints that a {@link Service} exposes.
 */
public class GetServiceEndpointsCommand extends AbstractAuthCommand implements Categorized {

  private final ServiceClient serviceClient;

  @Inject
  public GetServiceEndpointsCommand(ServiceClient serviceClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.serviceClient = serviceClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    ServiceId serviceId = new ServiceId(parseProgramId(arguments, ElementType.SERVICE));
    List<ServiceHttpEndpoint> endpoints = serviceClient.getEndpoints(serviceId);

    Table table = Table.builder()
      .setHeader("method", "path")
      .setRows(endpoints, new RowMaker<ServiceHttpEndpoint>() {
        @Override
        public List<?> makeRow(ServiceHttpEndpoint endpoint) {
          return Lists.newArrayList(endpoint.getMethod(), endpoint.getPath());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get endpoints service <%s> [version <%s>]", ArgumentName.SERVICE, ArgumentName.APP_VERSION);
  }

  @Override
  public String getDescription() {
    return String.format("Lists the endpoints that %s exposes",
                         Fragment.of(Article.A, ElementType.SERVICE.getName()));
  }

  @Override
  public String getCategory() {
    return CommandCategory.APPLICATION_LIFECYCLE.getName();
  }
}
