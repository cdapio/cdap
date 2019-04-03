/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.connector;

import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.dataset.lib.FileSetArguments;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.common.Constants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * Internal batch sink used as a connector between pipeline phases.
 * Though this extends BatchSink, this will not be instantiated through the plugin framework, but will
 * be created explicitly through the application.
 *
 * The batch connector is just a PartitionedFileSet, where a partition is the name of a phase that wrote to it.
 * This way, multiple phases can have the same local PartitionedFileSet as a sink, and the source will read data
 * from all partitions.
 *
 * This is because we don't want this to show up as a plugin that users can select and use, and also because
 * it uses features not exposed in the etl api (local workflow datasets).
 *
 * TODO: improve storage format. It is currently a json of the record but that is obviously not ideal
 *
 * @param <T> type of input object
 */
public abstract class ConnectorSink<T> extends BatchSink<T, NullWritable, Text> {
  private final String datasetName;
  private final String phaseName;

  protected ConnectorSink(String datasetName, String phaseName) {
    this.datasetName = datasetName;
    this.phaseName = phaseName;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Map<String, String> arguments = new HashMap<>();
    FileSetArguments.setOutputPath(arguments, Constants.Connector.DATA_DIR + "/" + phaseName);
    context.addOutput(Output.ofDataset(datasetName, arguments));
  }
}
