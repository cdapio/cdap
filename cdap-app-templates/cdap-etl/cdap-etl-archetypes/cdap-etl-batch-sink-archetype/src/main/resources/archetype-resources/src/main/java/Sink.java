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
package $package;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.io.NullWritable;

/**
 * Batch Sink.
 */

@Plugin(type = "batchsink")
@Name("Sink")
@Description("Batch sink")
public class Sink extends BatchSink<StructuredRecord, byte[], NullWritable> {

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    // Make sure any properties that are expected are valid
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    // Get Config param and use to initialize
    // String param = config.param
    // Perform init operations, external operations etc.
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], NullWritable>> emitter) throws Exception {
   // Transform the records
   // emitter.emit(myval);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
   // Configure hadoop job before running in batch.
  }

}

