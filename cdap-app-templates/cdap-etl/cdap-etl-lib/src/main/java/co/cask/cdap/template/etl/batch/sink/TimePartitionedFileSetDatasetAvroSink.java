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
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.StructuredToAvroTransformer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Avro record to {@link TimePartitionedFileSet}
 */
@Plugin(type = "sink")
@Name("TPFSAvro")
@Description("Sink for a TimePartitionedFileSet that writes data in Avro format.")
public class TimePartitionedFileSetDatasetAvroSink extends
  TimePartitionedFileSetSink<AvroKey<GenericRecord>, NullWritable> {
  private static final String SCHEMA_DESC = "The Avro schema of the record being written to the Sink as a JSON " +
    "Object.";
  private StructuredToAvroTransformer recordTransformer;
  private final TPFSAvroSinkConfig config;

  public TimePartitionedFileSetDatasetAvroSink(TPFSAvroSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    String tpfsName = tpfsSinkConfig.name;
    String basePath = tpfsSinkConfig.basePath == null ? tpfsName : tpfsSinkConfig.basePath;
    pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), FileSetProperties.builder()
      .setBasePath(basePath)
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", (config.schema))
      .build());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    super.prepareRun(context);
    Schema avroSchema = new Schema.Parser().parse(config.schema);
    Job job = context.getHadoopJob();
    AvroJob.setOutputKeySchema(job, avroSchema);
  }


  @Override
  public void initialize(BatchSinkContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  /**
   * Config for TimePartitionedFileSetAvroSink
   */
  public static class TPFSAvroSinkConfig extends TPFSSinkConfig {

    @Description(SCHEMA_DESC)
    private String schema;

    public TPFSAvroSinkConfig(String name, String schema, @Nullable String basePath) {
      super(name, basePath);
      this.schema = schema;
    }
  }

}
