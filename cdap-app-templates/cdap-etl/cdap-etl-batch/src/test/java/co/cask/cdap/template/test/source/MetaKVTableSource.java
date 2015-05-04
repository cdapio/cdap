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

package co.cask.cdap.template.test.source;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.batch.source.KVTableSource;

/**
 * Test Batch Source that writes to a table in {@link BatchSource#prepareRun} and {@link BatchSource#onRunFinish}.
 */
@Plugin(type = "source")
@Name("MetaKVTable")
public class MetaKVTableSource extends KVTableSource {
  public static final String META_TABLE = "source.meta.table";
  public static final String PREPARE_RUN_KEY = "source.prepare.run";
  public static final String FINISH_RUN_KEY = "source.finish.run";

  public MetaKVTableSource(KVTableConfig kvTableConfig) {
    super(kvTableConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.createDataset(META_TABLE, KeyValueTable.class, DatasetProperties.EMPTY);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    super.prepareRun(context);
    KeyValueTable table = context.getDataset(META_TABLE);
    table.write(PREPARE_RUN_KEY, PREPARE_RUN_KEY);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    super.onRunFinish(succeeded, context);
    KeyValueTable table = context.getDataset(META_TABLE);
    table.write(FINISH_RUN_KEY, FINISH_RUN_KEY);
  }
}
