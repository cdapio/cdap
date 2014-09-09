/*
 * Copyright 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.shell.command.deploy;

import co.cask.cdap.client.DatasetModuleClient;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.command.AbstractCommand;
import co.cask.cdap.shell.command.ArgumentName;
import co.cask.cdap.shell.command.Arguments;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.PrintStream;
import javax.inject.Inject;

/**
 * Deploys a dataset module.
 */
public class DeployDatasetModuleCommand extends AbstractCommand {

  private final DatasetModuleClient datasetModuleClient;

  @Inject
  public DeployDatasetModuleCommand(DatasetModuleClient datasetModuleClient) {
    this.datasetModuleClient = datasetModuleClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    File moduleJarFile = new File(arguments.get(ArgumentName.DATASET_MODULE_JAR_FILE));
    Preconditions.checkArgument(moduleJarFile.exists(),
                                "Module jar file '" + moduleJarFile.getAbsolutePath() + "' does not exist");
    Preconditions.checkArgument(moduleJarFile.canRead());
    String moduleName = arguments.get(ArgumentName.NEW_DATASET_MODULE);
    String moduleJarClassname = arguments.get(ArgumentName.DATASET_MODULE_JAR_CLASSNAME);

    datasetModuleClient.add(moduleName, moduleJarClassname, moduleJarFile);
    output.printf("Successfully deployed dataset module '%s'\n", moduleName);
  }

  @Override
  public String getPattern() {
    return String.format("deploy dataset module <%s> <%s> <%s>",
                         ArgumentName.NEW_DATASET_MODULE,
                         ArgumentName.DATASET_MODULE_JAR_FILE,
                         ArgumentName.DATASET_MODULE_JAR_CLASSNAME);
  }

  @Override
  public String getDescription() {
    return "Deploys a " + ElementType.DATASET_MODULE.getPrettyName();
  }
}
