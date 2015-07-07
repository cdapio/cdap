/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.util;

import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;

import java.io.PrintStream;

/**
 * Abstract command for updating {@link co.cask.cdap.security.authentication.client.AccessToken}.
 */
public abstract class AbstractAuthCommand implements Command {

  protected final CLIConfig cliConfig;

  public AbstractAuthCommand(CLIConfig cliConfig) {
    this.cliConfig = cliConfig;
  }

  @Override
  public void execute(Arguments arguments, PrintStream printStream) throws Exception {
    try {
      perform(arguments, printStream);
    } catch (UnauthorizedException e) {
      cliConfig.updateAccessToken(printStream);
      perform(arguments, printStream);
    }
  }

  public abstract void perform(Arguments arguments, PrintStream printStream) throws Exception;
}
