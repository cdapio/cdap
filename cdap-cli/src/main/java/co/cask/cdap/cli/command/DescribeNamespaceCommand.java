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
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;

/**
 * {@link Command} to describe a namespace.
 */
public class DescribeNamespaceCommand implements Command {

  private final NamespaceClient namespaceClient;
  private final TableRenderer tableRenderer;

  @Inject
  public DescribeNamespaceCommand(NamespaceClient namespaceClient, TableRenderer tableRenderer) {
    this.namespaceClient = namespaceClient;
    this.tableRenderer = tableRenderer;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    Id.Namespace namespace = Id.Namespace.from(arguments.get(ArgumentName.NAMESPACE_ID.getName()));
    NamespaceMeta namespaceMeta = namespaceClient.get(namespace.getId());
    Table table = Table.builder()
      .setHeader("id", "display_name", "description")
      .setRows(ImmutableList.of(namespaceMeta), new RowMaker<NamespaceMeta>() {
        @Override
        public List<?> makeRow(NamespaceMeta object) {
          return ImmutableList.of(object.getId(), object.getName(), object.getDescription());
        }
      }).build();
    tableRenderer.render(output, table);
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
