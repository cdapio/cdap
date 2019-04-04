/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.cli.command.app;

import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Deletes an application.
 */
public class DeleteAppCommand extends AbstractAuthCommand {

  private final ApplicationClient appClient;

  @Inject
  public DeleteAppCommand(ApplicationClient appClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.appClient = appClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    ApplicationId appId = parseApplicationId(arguments);
    appClient.delete(appId);
    output.printf("Successfully deleted application '%s.%s'\n", appId.getEntityName(), appId.getVersion());
  }

  @Override
  public String getPattern() {
    return String.format("delete app <%s> [version <%s>]", ArgumentName.APP, ArgumentName.APP_VERSION);
  }

  @Override
  public String getDescription() {
    return String.format("Deletes %s with an optional version. If version is not provided, default version '%s' " +
                           "will be used.", Fragment.of(Article.A, ElementType.APP.getName()),
                         ApplicationId.DEFAULT_VERSION);
  }
}
