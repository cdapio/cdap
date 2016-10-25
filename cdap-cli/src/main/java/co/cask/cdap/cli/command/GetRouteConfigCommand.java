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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.common.cli.Arguments;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.Map;

/**
 * Gets RouteConfig for a service.
 */
public class GetRouteConfigCommand extends AbstractAuthCommand {

  private static final Gson GSON = new Gson();
  private final ServiceClient serviceClient;

  @Inject
  public GetRouteConfigCommand(ServiceClient serviceClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.serviceClient = serviceClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    ServiceId serviceId = new ServiceId(parseProgramId(arguments, ElementType.SERVICE));
    Map<String, Integer> routeConfig = serviceClient.getRouteConfig(serviceId);
    output.printf(GSON.toJson(routeConfig));
  }

  @Override
  public String getPattern() {
    return String.format("get route-config for service <%s>", ArgumentName.SERVICE);
  }

  @Override
  public String getDescription() {
    return String.format("Get the route configuration for %s.", Fragment.of(Article.A, ElementType.SERVICE.getName()));
  }
}
