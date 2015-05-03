package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;

/**
 *
 */
@Plugin(type = "sink")
@Name("MetaKVSink")
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

