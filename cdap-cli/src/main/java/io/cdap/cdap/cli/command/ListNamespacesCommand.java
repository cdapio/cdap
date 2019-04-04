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

package io.cdap.cdap.cli.command;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.proto.NamespaceConfig;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.common.cli.Arguments;
import io.cdap.common.cli.Command;

import java.io.PrintStream;
import java.util.List;

/**
 * {@link Command} to list namespaces.
 * Uses {@link NamespaceCommandUtils#prettyPrintNamespaceConfigCLI(NamespaceConfig)} to display the
 * {@link NamespaceConfig}.
 */
public class ListNamespacesCommand extends AbstractCommand {

  private final NamespaceClient namespaceClient;

  @Inject
  public ListNamespacesCommand(CLIConfig cliConfig, NamespaceClient namespaceClient) {
    super(cliConfig);
    this.namespaceClient = namespaceClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    Table table = Table.builder()
      .setHeader("name", "description", "config")
      .setRows(namespaceClient.list(), new RowMaker<NamespaceMeta>() {
        @Override
        public List<?> makeRow(NamespaceMeta object) {
          return Lists.newArrayList(object.getName(), object.getDescription(),
                                    NamespaceCommandUtils.prettyPrintNamespaceConfigCLI(object.getConfig()));
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return "list namespaces";
  }

  @Override
  public String getDescription() {
    return String.format("Lists all %s", ElementType.NAMESPACE.getNamePlural());
  }
}
