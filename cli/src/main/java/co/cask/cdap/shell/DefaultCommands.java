/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.shell;

import co.cask.cdap.shell.command.CallProcedureCommand;
import co.cask.cdap.shell.command.ConnectCommand;
import co.cask.cdap.shell.command.CreateDatasetInstanceCommand;
import co.cask.cdap.shell.command.CreateStreamCommand;
import co.cask.cdap.shell.command.DeleteAppCommand;
import co.cask.cdap.shell.command.DeleteDatasetInstanceCommand;
import co.cask.cdap.shell.command.DeleteDatasetModuleCommand;
import co.cask.cdap.shell.command.DeployAppCommand;
import co.cask.cdap.shell.command.DeployDatasetModuleCommand;
import co.cask.cdap.shell.command.DescribeAppCommand;
import co.cask.cdap.shell.command.DescribeDatasetModuleCommand;
import co.cask.cdap.shell.command.DescribeDatasetTypeCommand;
import co.cask.cdap.shell.command.DescribeStreamCommand;
import co.cask.cdap.shell.command.ExecuteQueryCommand;
import co.cask.cdap.shell.command.GetProgramHistoryCommandSet;
import co.cask.cdap.shell.command.GetProgramInstancesCommandSet;
import co.cask.cdap.shell.command.GetProgramLiveInfoCommandSet;
import co.cask.cdap.shell.command.GetProgramLogsCommandSet;
import co.cask.cdap.shell.command.GetProgramStatusCommandSet;
import co.cask.cdap.shell.command.GetStreamEventsCommand;
import co.cask.cdap.shell.command.ListAllProgramsCommand;
import co.cask.cdap.shell.command.ListAppsCommand;
import co.cask.cdap.shell.command.ListDatasetInstancesCommand;
import co.cask.cdap.shell.command.ListDatasetModulesCommand;
import co.cask.cdap.shell.command.ListDatasetTypesCommand;
import co.cask.cdap.shell.command.ListProgramsCommandSet;
import co.cask.cdap.shell.command.ListStreamsCommand;
import co.cask.cdap.shell.command.SendStreamEventCommand;
import co.cask.cdap.shell.command.SetProgramInstancesCommandSet;
import co.cask.cdap.shell.command.SetStreamTTLCommand;
import co.cask.cdap.shell.command.StartProgramCommandSet;
import co.cask.cdap.shell.command.StopProgramCommandSet;
import co.cask.cdap.shell.command.TruncateDatasetInstanceCommand;
import co.cask.cdap.shell.command.TruncateStreamCommand;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Default set of commands.
 */
public class DefaultCommands extends CommandSet {

  @Inject
  public DefaultCommands(Injector injector) {
    super(
      injector.getInstance(CallProcedureCommand.class),
      injector.getInstance(ConnectCommand.class),
      injector.getInstance(CreateDatasetInstanceCommand.class),
      injector.getInstance(CreateStreamCommand.class),
      injector.getInstance(DeleteAppCommand.class),
      injector.getInstance(DeleteDatasetInstanceCommand.class),
      injector.getInstance(DeleteDatasetModuleCommand.class),
      injector.getInstance(DeployAppCommand.class),
      injector.getInstance(DeployDatasetModuleCommand.class),
      injector.getInstance(DescribeAppCommand.class),
      injector.getInstance(DescribeDatasetModuleCommand.class),
      injector.getInstance(DescribeDatasetTypeCommand.class),
      injector.getInstance(DescribeStreamCommand.class),
      injector.getInstance(ExecuteQueryCommand.class),
      injector.getInstance(GetProgramHistoryCommandSet.class),
      injector.getInstance(GetProgramInstancesCommandSet.class),
      injector.getInstance(GetProgramLiveInfoCommandSet.class),
      injector.getInstance(GetProgramLogsCommandSet.class),
      injector.getInstance(GetProgramStatusCommandSet.class),
      injector.getInstance(GetStreamEventsCommand.class),
      injector.getInstance(ListAllProgramsCommand.class),
      injector.getInstance(ListAppsCommand.class),
      injector.getInstance(ListDatasetInstancesCommand.class),
      injector.getInstance(ListDatasetModulesCommand.class),
      injector.getInstance(ListDatasetTypesCommand.class),
      injector.getInstance(ListProgramsCommandSet.class),
      injector.getInstance(ListStreamsCommand.class),
      injector.getInstance(SendStreamEventCommand.class),
      injector.getInstance(SetProgramInstancesCommandSet.class),
      injector.getInstance(SetStreamTTLCommand.class),
      injector.getInstance(StartProgramCommandSet.class),
      injector.getInstance(StopProgramCommandSet.class),
      injector.getInstance(TruncateDatasetInstanceCommand.class),
      injector.getInstance(TruncateStreamCommand.class)
    );
  }
}
