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

package io.cdap.cdap.cli.commandset;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.cli.Categorized;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.cdap.cli.command.CreateNamespaceCommand;
import io.cdap.cdap.cli.command.DeleteNamespaceCommand;
import io.cdap.cdap.cli.command.DescribeNamespaceCommand;
import io.cdap.cdap.cli.command.ListNamespacesCommand;
import io.cdap.cdap.cli.command.UseNamespaceCommand;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

/**
 * Namespace commands.
 */
public class NamespaceCommands extends CommandSet<Command> implements Categorized {

  @Inject
  public NamespaceCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(UseNamespaceCommand.class))
        .add(injector.getInstance(CreateNamespaceCommand.class))
        .add(injector.getInstance(ListNamespacesCommand.class))
        .add(injector.getInstance(DescribeNamespaceCommand.class))
        .add(injector.getInstance(DeleteNamespaceCommand.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.NAMESPACE.getName();
  }
}
