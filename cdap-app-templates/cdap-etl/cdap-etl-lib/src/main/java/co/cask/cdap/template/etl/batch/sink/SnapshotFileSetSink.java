/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link FileSetSink} that stores the data of the latest run of an adapter in HDFS.
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class SnapshotFileSetSink<KEY_OUT, VAL_OUT> extends FileSetSink<KEY_OUT, VAL_OUT> {
  private final FileSetSinkConfig config;
  protected Map<String, String> sinkArgs;

  public SnapshotFileSetSink(FileSetSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    sinkArgs = new HashMap<>();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-hh-mm");
    FileSetArguments.setOutputPath(sinkArgs, format.format(context.getLogicalStartTime()));
    context.addOutput(config.name, sinkArgs);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    FileSet fileSet = context.getDataset(config.name, sinkArgs);
    Location curLocation = fileSet.getOutputLocation();
    try {
      Location targetLocation = fileSet.getBaseLocation().append(config.path);
      if (!targetLocation.exists()) {
        targetLocation.mkdirs();
      }
      Location temp = fileSet.getBaseLocation().getTempFile(".tmp");
      targetLocation.renameTo(temp);
      curLocation.renameTo(targetLocation);
      temp.delete(true);
    } catch (IOException e) {
      //necessary because onRunFinish doesn't throw exceptions
      throw new RuntimeException(e);
    }
  }
}
