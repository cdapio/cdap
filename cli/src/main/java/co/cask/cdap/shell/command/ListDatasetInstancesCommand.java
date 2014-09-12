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

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.util.AsciiTable;
import co.cask.cdap.shell.util.RowMaker;

import java.io.PrintStream;
import java.util.List;
import javax.inject.Inject;

/**
 * Lists datasets.
 */
public class ListDatasetInstancesCommand extends AbstractCommand {

  private final DatasetClient datasetClient;

  @Inject
  public ListDatasetInstancesCommand(DatasetClient datasetClient) {
    super("instances", null, "Lists all " + ElementType.DATASET.getPluralPrettyName());
    this.datasetClient = datasetClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    List<DatasetSpecification> datasetMetas = datasetClient.list();

    new AsciiTable<DatasetSpecification>(
      new String[]{"name", "type"}, datasetMetas,
      new RowMaker<DatasetSpecification>() {
        @Override
        public Object[] makeRow(DatasetSpecification object) {
          return new Object[] { object.getName(), object.getType() };
        }
      }).print(output);
  }
}
