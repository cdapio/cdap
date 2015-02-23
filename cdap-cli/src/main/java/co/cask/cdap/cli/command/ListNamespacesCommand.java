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

import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * {@link Command} to list namespaces.
 */
public class ListNamespacesCommand implements Command {

  private final NamespaceClient namespaceClient;
  private final TableRenderer tableRenderer;

  @Inject
  public ListNamespacesCommand(NamespaceClient namespaceClient, TableRenderer tableRenderer) {
    this.namespaceClient = namespaceClient;
    this.tableRenderer = tableRenderer;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    Table table = Table.builder()
      .setHeader("id", "display_name", "description")
      .setRows(namespaceClient.list(), new RowMaker<NamespaceMeta>() {
        @Override
        public Object[] makeRow(NamespaceMeta object) {
          return new Object[] {object.getId(), object.getName(), object.getDescription()};
        }
      }).build();
    tableRenderer.render(output, table);
  }

  @Override
  public String getPattern() {
    return "list namespaces";
  }

  @Override
  public String getDescription() {
    return String.format("Lists all %s.", ElementType.NAMESPACE.getPluralPrettyName());
  }
}
