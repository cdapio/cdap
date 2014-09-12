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

package co.cask.cdap.shell.command;

import co.cask.cdap.client.DatasetModuleClient;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.completer.Completable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import javax.inject.Inject;

/**
 * Deploys a dataset module.
 */
public class DeployDatasetModuleCommand extends AbstractCommand implements Completable {

  private final DatasetModuleClient datasetModuleClient;

  @Inject
  public DeployDatasetModuleCommand(DatasetModuleClient datasetModuleClient) {
    super("module", "<module-jar-file> <module-name> <module-jar-classname>",
          "Deploys a " + ElementType.DATASET_MODULE.getPrettyName());
    this.datasetModuleClient = datasetModuleClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    File moduleJarFile = new File(args[0]);
    Preconditions.checkArgument(moduleJarFile.exists(),
                                "Module jar file '" + moduleJarFile.getAbsolutePath() + "' does not exist");
    Preconditions.checkArgument(moduleJarFile.canRead());
    String moduleName = args[1];
    String moduleJarClassname = args[2];

    datasetModuleClient.add(moduleName, moduleJarClassname, moduleJarFile);
    output.printf("Successfully deployed dataset module '%s'\n", moduleName);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, new FileNameCompleter()));
  }
}
