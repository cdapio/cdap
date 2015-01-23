/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.cli;

import co.cask.cdap.cli.command.CallProcedureCommand;
import co.cask.cdap.cli.command.CallServiceCommand;
import co.cask.cdap.cli.command.ConnectCommand;
import co.cask.cdap.cli.command.CreateAdapterCommand;
import co.cask.cdap.cli.command.CreateDatasetInstanceCommand;
import co.cask.cdap.cli.command.CreateNamespaceCommand;
import co.cask.cdap.cli.command.CreateStreamCommand;
import co.cask.cdap.cli.command.DeleteAdapterCommand;
import co.cask.cdap.cli.command.DeleteAppCommand;
import co.cask.cdap.cli.command.DeleteDatasetInstanceCommand;
import co.cask.cdap.cli.command.DeleteDatasetModuleCommand;
import co.cask.cdap.cli.command.DeleteNamespaceCommand;
import co.cask.cdap.cli.command.DeployAppCommand;
import co.cask.cdap.cli.command.DeployDatasetModuleCommand;
import co.cask.cdap.cli.command.DescribeAppCommand;
import co.cask.cdap.cli.command.DescribeDatasetModuleCommand;
import co.cask.cdap.cli.command.DescribeDatasetTypeCommand;
import co.cask.cdap.cli.command.DescribeNamespaceCommand;
import co.cask.cdap.cli.command.DescribeStreamCommand;
import co.cask.cdap.cli.command.ExecuteQueryCommand;
import co.cask.cdap.cli.command.ExitCommand;
import co.cask.cdap.cli.command.GetProgramInstancesCommandSet;
import co.cask.cdap.cli.command.GetProgramLiveInfoCommandSet;
import co.cask.cdap.cli.command.GetProgramLogsCommandSet;
import co.cask.cdap.cli.command.GetProgramRunsCommandSet;
import co.cask.cdap.cli.command.GetProgramRuntimeArgsCommandSet;
import co.cask.cdap.cli.command.GetProgramStatusCommandSet;
import co.cask.cdap.cli.command.GetServiceEndpointsCommand;
import co.cask.cdap.cli.command.GetStreamEventsCommand;
import co.cask.cdap.cli.command.ListAdaptersCommand;
import co.cask.cdap.cli.command.ListAllProgramsCommand;
import co.cask.cdap.cli.command.ListAppsCommand;
import co.cask.cdap.cli.command.ListDatasetInstancesCommand;
import co.cask.cdap.cli.command.ListDatasetModulesCommand;
import co.cask.cdap.cli.command.ListDatasetTypesCommand;
import co.cask.cdap.cli.command.ListNamespacesCommand;
import co.cask.cdap.cli.command.ListProgramsCommandSet;
import co.cask.cdap.cli.command.ListStreamsCommand;
import co.cask.cdap.cli.command.LoadStreamCommand;
import co.cask.cdap.cli.command.PreferencesCommandSet;
import co.cask.cdap.cli.command.SendStreamEventCommand;
import co.cask.cdap.cli.command.SetProgramInstancesCommandSet;
import co.cask.cdap.cli.command.SetProgramRuntimeArgsCommandSet;
import co.cask.cdap.cli.command.SetStreamFormatCommand;
import co.cask.cdap.cli.command.SetStreamPropertiesCommand;
import co.cask.cdap.cli.command.SetStreamTTLCommand;
import co.cask.cdap.cli.command.StartProgramCommandSet;
import co.cask.cdap.cli.command.StopProgramCommandSet;
import co.cask.cdap.cli.command.TruncateDatasetInstanceCommand;
import co.cask.cdap.cli.command.TruncateStreamCommand;
import co.cask.cdap.cli.command.UseNamespaceCommand;
import co.cask.cdap.cli.command.VersionCommand;
import co.cask.common.cli.Command;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;

import java.util.List;

/**
 * Default set of commands.
 */
public class DefaultCommands implements Supplier<List<Command>> {

  private final List<Command> commands;

  @Inject
  public DefaultCommands(Injector injector) {
    this.commands = ImmutableList.<Command>builder()
      .add(injector.getInstance(VersionCommand.class))
      .add(injector.getInstance(ExitCommand.class))
      .add(injector.getInstance(UseNamespaceCommand.class))
      .add(injector.getInstance(CallProcedureCommand.class))
      .add(injector.getInstance(ConnectCommand.class))
      .add(injector.getInstance(CreateDatasetInstanceCommand.class))
      .add(injector.getInstance(CreateStreamCommand.class))
      .add(injector.getInstance(DeleteAppCommand.class))
      .add(injector.getInstance(DeleteDatasetInstanceCommand.class))
      .add(injector.getInstance(DeleteDatasetModuleCommand.class))
      .add(injector.getInstance(DeployAppCommand.class))
      .add(injector.getInstance(DeployDatasetModuleCommand.class))
      .add(injector.getInstance(DescribeAppCommand.class))
      .add(injector.getInstance(DescribeDatasetModuleCommand.class))
      .add(injector.getInstance(DescribeDatasetTypeCommand.class))
      .add(injector.getInstance(DescribeStreamCommand.class))
      .add(injector.getInstance(ExecuteQueryCommand.class))
      .addAll(injector.getInstance(GetProgramRunsCommandSet.class).getCommands())
      .addAll(injector.getInstance(GetProgramInstancesCommandSet.class).getCommands())
      .addAll(injector.getInstance(GetProgramLiveInfoCommandSet.class).getCommands())
      .addAll(injector.getInstance(GetProgramLogsCommandSet.class).getCommands())
      .addAll(injector.getInstance(GetProgramStatusCommandSet.class).getCommands())
      .addAll(injector.getInstance(GetProgramRuntimeArgsCommandSet.class).getCommands())
      .addAll(injector.getInstance(SetProgramRuntimeArgsCommandSet.class).getCommands())
      .add(injector.getInstance(GetStreamEventsCommand.class))
      .addAll(injector.getInstance(PreferencesCommandSet.class).getCommands())
      .add(injector.getInstance(ListAllProgramsCommand.class))
      .add(injector.getInstance(ListAppsCommand.class))
      .add(injector.getInstance(ListDatasetInstancesCommand.class))
      .add(injector.getInstance(ListDatasetModulesCommand.class))
      .add(injector.getInstance(ListDatasetTypesCommand.class))
      .addAll(injector.getInstance(ListProgramsCommandSet.class).getCommands())
      .add(injector.getInstance(ListStreamsCommand.class))
      .add(injector.getInstance(SendStreamEventCommand.class))
      .add(injector.getInstance(LoadStreamCommand.class))
      .addAll(injector.getInstance(SetProgramInstancesCommandSet.class).getCommands())
      .add(injector.getInstance(SetStreamTTLCommand.class))
      .add(injector.getInstance(SetStreamFormatCommand.class))
      .add(injector.getInstance(SetStreamPropertiesCommand.class))
      .addAll(injector.getInstance(StartProgramCommandSet.class).getCommands())
      .addAll(injector.getInstance(StopProgramCommandSet.class).getCommands())
      .add(injector.getInstance(TruncateDatasetInstanceCommand.class))
      .add(injector.getInstance(TruncateStreamCommand.class))
      .add(injector.getInstance(CallServiceCommand.class))
      .add(injector.getInstance(GetServiceEndpointsCommand.class))
      // namespaces
      .add(injector.getInstance(CreateNamespaceCommand.class))
      .add(injector.getInstance(ListNamespacesCommand.class))
      .add(injector.getInstance(DescribeNamespaceCommand.class))
      .add(injector.getInstance(DeleteNamespaceCommand.class))
      // adapters
      .add(injector.getInstance(ListAdaptersCommand.class))
      .add(injector.getInstance(CreateAdapterCommand.class))
      .add(injector.getInstance(DeleteAdapterCommand.class))
      .build();
  }

  @Override
  public List<Command> get() {
    return commands;
  }
}
