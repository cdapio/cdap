/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.proto.Id;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.inject.Inject;
import jline.console.ConsoleReader;

import java.io.PrintStream;

/**
 * {@link Command} to delete a namespace.
 */
public class DeleteNamespaceCommand extends AbstractCommand {
  private static final String SUCCESS_MSG = "Namespace '%s' deleted successfully.";
  private final NamespaceClient namespaceClient;
  private final CLIConfig cliConfig;

  @Inject
  public DeleteNamespaceCommand(CLIConfig cliConfig, NamespaceClient namespaceClient) {
    super(cliConfig);
    this.cliConfig = cliConfig;
    this.namespaceClient = namespaceClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream out) throws Exception {
    Id.Namespace namespaceId = Id.Namespace.from(arguments.get(ArgumentName.NAMESPACE_NAME.toString()));

    ConsoleReader consoleReader = new ConsoleReader();
    if (Id.Namespace.DEFAULT.equals(namespaceId)) {
      out.println("WARNING: Deleting contents of a namespace is an unrecoverable operation");
      String prompt = String.format("Are you sure you want to delete contents of namespace '%s' [y/N]? ",
                                    namespaceId.getId());
      String userConfirm = consoleReader.readLine(prompt);
      if ("y".equalsIgnoreCase(userConfirm)) {
        namespaceClient.delete(namespaceId);
        out.printf("Contents of namespace '%s' were deleted successfully", namespaceId.getId());
        out.println();
      }
    } else {
      out.println("WARNING: Deleting a namespace is an unrecoverable operation");
      String prompt = String.format("Are you sure you want to delete namespace '%s' [y/N]? ",
                                    namespaceId.getId());
      String userConfirm = consoleReader.readLine(prompt);
      if ("y".equalsIgnoreCase(userConfirm)) {
        namespaceClient.delete(namespaceId);
        out.println(String.format(SUCCESS_MSG, namespaceId));
        if (cliConfig.getCurrentNamespace().equals(namespaceId)) {
          cliConfig.setNamespace(Id.Namespace.DEFAULT);
          out.printf("Now using namespace '%s'", Id.Namespace.DEFAULT.getId());
          out.println();
        }
      }
    }
  }

  @Override
  public String getPattern() {
    return String.format("delete namespace <%s>", ArgumentName.NAMESPACE_NAME);
  }

  @Override
  public String getDescription() {
    return String.format("Deletes %s.", Fragment.of(Article.A, ElementType.NAMESPACE.getName()));
  }
}
