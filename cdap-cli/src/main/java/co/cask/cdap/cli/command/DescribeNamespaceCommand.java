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
import co.cask.cdap.cli.util.AsciiTable;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * {@link Command} to describe a namespace.
 */
public class DescribeNamespaceCommand implements Command {
  private final NamespaceClient namespaceClient;

  @Inject
  public DescribeNamespaceCommand(NamespaceClient namespaceClient) {
    this.namespaceClient = namespaceClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    NamespaceMeta namespaceMeta = namespaceClient.get(arguments.get(ArgumentName.NAMESPACE_ID.getName()));
    new AsciiTable<NamespaceMeta>(
      new String[]{"id", "display_name", "description"},
      Lists.newArrayList(namespaceMeta),
      new RowMaker<NamespaceMeta>() {
        @Override
        public Object[] makeRow(NamespaceMeta object) {
          return new Object[] {object.getId(), object.getDisplayName(), object.getDescription()};
        }
      }
    ).print(output);
  }

  @Override
  public String getPattern() {
    return String.format("describe namespace <%s>", ArgumentName.NAMESPACE_ID);
  }

  @Override
  public String getDescription() {
    return String.format("Describes a %s.", ElementType.NAMESPACE.getPrettyName());
  }
}
