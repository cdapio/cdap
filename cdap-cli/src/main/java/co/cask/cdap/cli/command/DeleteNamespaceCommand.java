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
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.proto.Id;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * {@link Command} to delete a namespace.
 */
public class DeleteNamespaceCommand implements Command {
  private static final String SUCCESS_MSG = "Namespace '%s' deleted successfully.";
  private final NamespaceClient namespaceClient;

  @Inject
  public DeleteNamespaceCommand(NamespaceClient namespaceClient) {
    this.namespaceClient = namespaceClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream out) throws Exception {
    Id.Namespace namespace = Id.Namespace.from(arguments.get(ArgumentName.NAMESPACE_ID.toString()));
    namespaceClient.delete(namespace);
    out.println(String.format(SUCCESS_MSG, namespace.getId()));
  }

  @Override
  public String getPattern() {
    return String.format("delete namespace <%s>", ArgumentName.NAMESPACE_ID);
  }

  @Override
  public String getDescription() {
    return String.format("Deletes a %s.", ElementType.NAMESPACE.getPrettyName());
  }
}
