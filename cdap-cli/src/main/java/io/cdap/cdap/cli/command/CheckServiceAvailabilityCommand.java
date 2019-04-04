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
 * the License
 */

package io.cdap.cdap.cli.command;

import com.google.inject.Inject;
import io.cdap.cdap.api.service.Service;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.Categorized;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.client.ServiceClient;
import io.cdap.cdap.proto.id.ServiceId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Check whether a {@link Service} has reached active status.
 */
public class CheckServiceAvailabilityCommand extends AbstractAuthCommand implements Categorized {
  private final ServiceClient serviceClient;

  @Inject
  public CheckServiceAvailabilityCommand(ServiceClient serviceClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.serviceClient = serviceClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    ServiceId serviceId = new ServiceId(parseProgramId(arguments, ElementType.SERVICE));
    serviceClient.checkAvailability(serviceId);
    output.println("Service is available to accept requests.");
  }

  @Override
  public String getPattern() {
    return String.format("check service availability <%s> [version <%s>]", ArgumentName.SERVICE,
                         ArgumentName.APP_VERSION);
  }

  @Override
  public String getDescription() {
    return String.format("Check if %s is available to accept requests",
                         Fragment.of(Article.A, ElementType.SERVICE.getName()));
  }

  @Override
  public String getCategory() {
    return CommandCategory.APPLICATION_LIFECYCLE.getName();
  }
}
