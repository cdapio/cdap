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

package co.cask.cdap.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.Properties;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link FileBatchSink} that stores the data of the latest run of an adapter in HDFS.
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class SnapshotFileBatchSink<KEY_OUT, VAL_OUT> extends FileBatchSink<KEY_OUT, VAL_OUT> {
  private static final String PATH_EXTENSION_DESCRIPTION = "The extension where the snapshot will be stored. " +
    "The snapshot will be stored at <basePath>/<extension>";

  private final SnapshotFileConfig config;
  protected Map<String, String> sinkArgs;

  public SnapshotFileBatchSink(SnapshotFileConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    sinkArgs = getAdditionalFileSetArguments();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-hh-mm");
    FileSetArguments.setOutputPath(sinkArgs, format.format(context.getLogicalStartTime()));
    context.addOutput(config.name, sinkArgs);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    FileSet fileSet = context.getDataset(config.name, sinkArgs);
    Location curLocation = fileSet.getOutputLocation();
    try {
      Location targetLocation = fileSet.getBaseLocation().append(config.pathExtension);
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

  /**
   * This base class will set the output path argument. Any additional arguments should be returned by this method.
   */
  protected Map<String, String> getAdditionalFileSetArguments() {
    //no-op
    return Collections.emptyMap();
  }

  /**
   * Config for snapshot file sets
   */
  public static class SnapshotFileConfig extends FileSetSinkConfig {
    @Name(Properties.SnapshotFileSet.PATH_EXTENSION)
    @Description(PATH_EXTENSION_DESCRIPTION)
    protected String pathExtension;

    public SnapshotFileConfig(String name, @Nullable String basePath, String pathExtension) {
      super(name, basePath);
      this.pathExtension = pathExtension;
    }
  }
}
