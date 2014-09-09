/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command.delete;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.ArgumentName;
import co.cask.cdap.shell.command.Arguments;

import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Deletes an application.
 */
public class DeleteAppCommand extends AbstractCommand {

  private final ApplicationClient appClient;

  @Inject
  public DeleteAppCommand(ApplicationClient appClient) {
    this.appClient = appClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String appId = arguments.get(ArgumentName.APP);

    appClient.delete(appId);
    output.printf("Successfully deleted application '%s'\n", appId);
  }

  @Override
  public String getPattern() {
    return String.format("delete app <%s>", ArgumentName.APP);
  }

  @Override
  public String getDescription() {
    return "Deletes an " + ElementType.APP.getPrettyName();
  }
}
