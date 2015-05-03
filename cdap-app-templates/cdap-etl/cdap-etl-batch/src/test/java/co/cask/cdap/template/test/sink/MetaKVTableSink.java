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

package co.cask.cdap.template.test.sink;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.batch.sink.KVTableSink;

/**
 * Test Batch Sink that writes to a table in {@link BatchSink#prepareRun} and {@link BatchSink#onRunFinish}.
 */
@Plugin(type = "sink")
@Name("MetaKVTable")
public class MetaKVTableSink extends KVTableSink {
  public static final String META_TABLE = "sink.meta.table";
  public static final String PREPARE_RUN_KEY = "sink.prepare.run";
  public static final String FINISH_RUN_KEY = "sink.finish.run";

  public MetaKVTableSink(KVTableConfig kvTableConfig) {
    super(kvTableConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.createDataset(META_TABLE, KeyValueTable.class, DatasetProperties.EMPTY);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    super.prepareRun(context);
    KeyValueTable table = context.getDataset(META_TABLE);
    table.write(PREPARE_RUN_KEY, PREPARE_RUN_KEY);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    super.onRunFinish(succeeded, context);
    KeyValueTable table = context.getDataset(META_TABLE);
    table.write(FINISH_RUN_KEY, FINISH_RUN_KEY);
  }
}
