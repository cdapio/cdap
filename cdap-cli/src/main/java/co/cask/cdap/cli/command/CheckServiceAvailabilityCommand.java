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

package co.cask.cdap.cli.command;

import co.cask.cdap.api.service.Service;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.ServiceClient;
import co.cask.cdap.proto.id.ServiceId;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;

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
    ServiceId serviceId = parseServiceId(arguments);
    serviceClient.checkAvailability(serviceId);
    output.println("Service is available to accept requests.");
  }

  @Override
  public String getPattern() {
    return String.format("check service availability <%s>", ArgumentName.SERVICE);
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
