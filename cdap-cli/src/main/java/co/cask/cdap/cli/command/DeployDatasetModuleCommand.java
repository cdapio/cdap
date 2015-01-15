/*
 * Copyright © 2012-2014 Cask Data, Inc.
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
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.DatasetModuleClient;
import co.cask.common.cli.Arguments;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import java.io.File;
import java.io.PrintStream;

/**
 * Deploys a dataset module.
 */
public class DeployDatasetModuleCommand extends AbstractAuthCommand {

  private final DatasetModuleClient datasetModuleClient;
  private final FilePathResolver resolver;

  @Inject
  public DeployDatasetModuleCommand(DatasetModuleClient datasetModuleClient, FilePathResolver resolver,
                                    CLIConfig cliConfig) {
    super(cliConfig);
    this.datasetModuleClient = datasetModuleClient;
    this.resolver = resolver;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    File moduleJarFile = resolver.resolvePathToFile(arguments.get(ArgumentName.DATASET_MODULE_JAR_FILE.toString()));
    Preconditions.checkArgument(moduleJarFile.exists(),
                                "Module jar file '" + moduleJarFile.getAbsolutePath() + "' does not exist");
    Preconditions.checkArgument(moduleJarFile.canRead());
    String moduleName = arguments.get(ArgumentName.NEW_DATASET_MODULE.toString());
    String moduleJarClassname = arguments.get(ArgumentName.DATASET_MODULE_JAR_CLASSNAME.toString());

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
