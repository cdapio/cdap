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

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;

import java.util.HashMap;

/**
 * File Batch Sink class that stores data in a file
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class FileBatchSink<KEY_OUT, VAL_OUT> extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  public static final String NAME_DESC = "Name of the Fileset Dataset to which the records " +
    "are written to. If it doesn't exist, it will be created.";
  public static final String BASE_PATH_DESC = "The path where the data will be recorded. " +
    "Defaults to the name of the dataset";

  protected final FileSetSinkConfig config;

  protected FileBatchSink(FileSetSinkConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    context.addOutput(config.name, new HashMap<String, String>());
  }
}
