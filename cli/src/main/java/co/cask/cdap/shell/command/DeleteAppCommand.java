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

package co.cask.cdap.shell.command;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.completer.Completable;
import co.cask.cdap.shell.completer.element.AppIdCompleter;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.List;
import javax.inject.Inject;

/**
 * Deletes an application.
 */
public class DeleteAppCommand extends AbstractCommand implements Completable {

  private final ApplicationClient appClient;
  private final AppIdCompleter completer;

  @Inject
  public DeleteAppCommand(AppIdCompleter completer, ApplicationClient appClient) {
    super("app", "<app-id>", "Deletes an " + ElementType.APP.getPrettyName());
    this.completer = completer;
    this.appClient = appClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String appId = args[0];

    appClient.delete(appId);
    output.printf("Successfully deleted application '%s'\n", appId);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completer));
  }
}
