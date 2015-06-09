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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.StructuredToAvroTransformer;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Avro record to {@link TimePartitionedFileSet}
 */
@Plugin(type = "sink")
@Name("TPFSAvro")
@Description("AVRO Sink with Time Partitioned File Dataset")
public class TimePartitionedFileSetDatasetAvroSink extends
  BatchSink<StructuredRecord, AvroKey<GenericRecord>, NullWritable> {

  private static final String SCHEMA_DESC = "The avro schema of the record being written to the Sink as a JSON Object";
  private static final String TPFS_NAME_DESC = "Name of the Time Partitioned FileSet Dataset to which the records " +
    "have to be written. If it doesn't exist, it will be created";
  private static final String BASE_PATH_DESC = "Base path for the time partitioned fileset. Defaults to the " +
    "name of the dataset";
  private final StructuredToAvroTransformer recordTransformer = new StructuredToAvroTransformer();

  /**
   * Config for TimePartitionedFileSetDatasetAvroSink
   */
  public static class TPFSAvroSinkConfig extends PluginConfig {

    @Description(TPFS_NAME_DESC)
    private String name;

    @Description(SCHEMA_DESC)
    private String schema;

    @Description(BASE_PATH_DESC)
    @Nullable
    private String basePath;
  }

  private final TPFSAvroSinkConfig tpfsAvroSinkConfig;

  public TimePartitionedFileSetDatasetAvroSink(TPFSAvroSinkConfig tpfsAvroSinkConfig) {
    this.tpfsAvroSinkConfig = tpfsAvroSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    String tpfsName = tpfsAvroSinkConfig.name;
    String basePath = tpfsAvroSinkConfig.basePath == null ? tpfsName : tpfsAvroSinkConfig.basePath;
    pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), FileSetProperties.builder()
      .setBasePath(basePath)
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", (tpfsAvroSinkConfig.schema))
      .build());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Map<String, String> sinkArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, context.getLogicalStartTime());
    TimePartitionedFileSet sink = context.getDataset(tpfsAvroSinkConfig.name, sinkArgs);
    context.setOutput(tpfsAvroSinkConfig.name, sink);
    Schema avroSchema = new Schema.Parser().parse(tpfsAvroSinkConfig.schema);
    Job job = context.getHadoopJob();
    AvroJob.setOutputKeySchema(job, avroSchema);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(
      new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }
}
