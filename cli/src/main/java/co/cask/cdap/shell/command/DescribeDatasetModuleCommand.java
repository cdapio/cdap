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
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.completer.Completable;
import co.cask.cdap.shell.completer.element.DatasetModuleNameCompleter;
import co.cask.cdap.shell.util.AsciiTable;
import co.cask.cdap.shell.util.RowMaker;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.List;
import javax.inject.Inject;

/**
 * Shows information about a dataset module.
 */
public class DescribeDatasetModuleCommand extends AbstractCommand implements Completable {

  private final DatasetModuleClient datasetModuleClient;
  private final DatasetModuleNameCompleter completer;

  @Inject
  public DescribeDatasetModuleCommand(DatasetModuleNameCompleter completer,
                                      DatasetModuleClient datasetModuleClient) {
    super("module", "<module-name>", "Shows information about a " + ElementType.DATASET_MODULE.getPrettyName());
    this.completer = completer;
    this.datasetModuleClient = datasetModuleClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String moduleName = args[0];
    DatasetModuleMeta datasetModuleMeta = datasetModuleClient.get(moduleName);

    new AsciiTable<DatasetModuleMeta>(
      new String[] { "name", "className", "jarLocation", "types", "usesModules", "usedByModules" },
      Lists.newArrayList(datasetModuleMeta),
      new RowMaker<DatasetModuleMeta>() {
        @Override
        public Object[] makeRow(DatasetModuleMeta object) {
          return new Object[] { object.getName(), object.getClassName(), object.getJarLocation(),
            Joiner.on(", ").join(object.getTypes()),
            Joiner.on(", ").join(object.getUsesModules()),
            Joiner.on(", ").join(object.getUsedByModules())
          };
        }
      }).print(output);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completer));
  }
}
