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

import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.shell.AbstractCommand;
import co.cask.cdap.shell.ElementType;
import co.cask.cdap.shell.completer.Completable;
import co.cask.cdap.shell.completer.element.DatasetNameCompleter;
import com.google.common.collect.Lists;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.List;
import javax.inject.Inject;

/**
 * Truncates a dataset.
 */
public class TruncateDatasetInstanceCommand extends AbstractCommand implements Completable {

  private final DatasetClient datasetClient;
  private final DatasetNameCompleter completer;

  @Inject
  public TruncateDatasetInstanceCommand(DatasetNameCompleter completer, DatasetClient datasetClient) {
    super("instance", "<dataset-name>", "Truncates a " + ElementType.DATASET.getPrettyName());
    this.completer = completer;
    this.datasetClient = datasetClient;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    super.process(args, output);

    String datasetName = args[0];
    datasetClient.truncate(datasetName);
    output.printf("Successfully truncated dataset '%s'\n", datasetName);
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    return Lists.newArrayList(prefixCompleter(prefix, completer));
  }
}
