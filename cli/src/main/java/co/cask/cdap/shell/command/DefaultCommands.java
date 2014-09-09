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

package co.cask.cdap.shell.command;

import co.cask.cdap.shell.command.call.CallProcedureCommand;
import co.cask.cdap.shell.command.connect.ConnectCommand;
import co.cask.cdap.shell.command.create.CreateDatasetInstanceCommand;
import co.cask.cdap.shell.command.create.CreateStreamCommand;
import co.cask.cdap.shell.command.delete.DeleteAppCommand;
import co.cask.cdap.shell.command.delete.DeleteDatasetInstanceCommand;
import co.cask.cdap.shell.command.delete.DeleteDatasetModuleCommand;
import co.cask.cdap.shell.command.deploy.DeployAppCommand;
import co.cask.cdap.shell.command.deploy.DeployDatasetModuleCommand;
import co.cask.cdap.shell.command.describe.DescribeAppCommand;
import co.cask.cdap.shell.command.describe.DescribeDatasetModuleCommand;
import co.cask.cdap.shell.command.describe.DescribeDatasetTypeCommand;
import co.cask.cdap.shell.command.describe.DescribeStreamCommand;
import co.cask.cdap.shell.command.execute.ExecuteQueryCommand;
import co.cask.cdap.shell.command.get.GetHistoryCommandSet;
import co.cask.cdap.shell.command.get.GetInstancesCommandSet;
import co.cask.cdap.shell.command.get.GetLiveInfoCommandSet;
import co.cask.cdap.shell.command.get.GetLogsCommandSet;
import co.cask.cdap.shell.command.get.GetStatusCommandSet;
import co.cask.cdap.shell.command.list.ListAllProgramsCommand;
import co.cask.cdap.shell.command.list.ListAppsCommand;
import co.cask.cdap.shell.command.list.ListDatasetInstancesCommand;
import co.cask.cdap.shell.command.list.ListDatasetModulesCommand;
import co.cask.cdap.shell.command.list.ListDatasetTypesCommand;
import co.cask.cdap.shell.command.list.ListProgramsCommandSet;
import co.cask.cdap.shell.command.list.ListStreamsCommand;
import co.cask.cdap.shell.command.send.SendStreamEventCommand;
import co.cask.cdap.shell.command.set.SetProgramInstancesCommandSet;
import co.cask.cdap.shell.command.set.SetStreamTTLCommand;
import co.cask.cdap.shell.command.start.StartProgramCommandSet;
import co.cask.cdap.shell.command.stop.StopProgramCommandSet;
import co.cask.cdap.shell.command.truncate.TruncateDatasetInstanceCommand;
import co.cask.cdap.shell.command.truncate.TruncateStreamCommand;
import com.google.inject.Injector;

import javax.inject.Inject;

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
      injector.getInstance(GetHistoryCommandSet.class),
      injector.getInstance(GetInstancesCommandSet.class),
      injector.getInstance(GetLiveInfoCommandSet.class),
      injector.getInstance(GetLogsCommandSet.class),
      injector.getInstance(GetStatusCommandSet.class),
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
