/*
 * Copyright 2014 Cask Data, Inc.
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
import co.cask.cdap.shell.util.AsciiTable;
import co.cask.cdap.shell.util.RowMaker;

import java.io.PrintStream;
import java.util.List;
import javax.inject.Inject;

/**
 * Lists all dataset modules.
 */
public class ListDatasetModulesCommand extends AbstractCommand {

  private final DatasetModuleClient client;

  @Inject
  public ListDatasetModulesCommand(DatasetModuleClient client) {
    super("modules", null, "Lists " + ElementType.DATASET_MODULE.getPluralPrettyName());
    this.client = client;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    List<DatasetModuleMeta> list = client.list();
    new AsciiTable<DatasetModuleMeta>(
      new String[] { "name", "className" },
      list,
      new RowMaker<DatasetModuleMeta>() {
        @Override
        public Object[] makeRow(DatasetModuleMeta object) {
          return new String[] { object.getName(), object.getClassName() };
        }
      }).print(output);
  }
}
