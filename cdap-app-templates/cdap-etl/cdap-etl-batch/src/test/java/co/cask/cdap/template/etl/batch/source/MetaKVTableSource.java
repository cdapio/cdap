package co.cask.cdap.template.etl.batch.source;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;

/**
 *
 */
@Plugin(type = "source")
@Name("MetaKVSource")
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
