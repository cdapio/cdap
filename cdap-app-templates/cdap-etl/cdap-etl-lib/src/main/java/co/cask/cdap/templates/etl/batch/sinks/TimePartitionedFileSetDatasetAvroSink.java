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

package co.cask.cdap.templates.etl.batch.sinks;

import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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

/**
 * A {@link BatchSink} to write Avro record to {@link TimePartitionedFileSet}
 */
public class TimePartitionedFileSetDatasetAvroSink extends
  BatchSink<GenericRecord, AvroKey<GenericRecord>, NullWritable> {

  private static final String TPFS_NAME = "name";
  private static final String SCHEMA = "schema";
  private static final String BASE_PATH = "basePath";

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(TimePartitionedFileSetDatasetAvroSink.class.getSimpleName());
    configurer.setDescription("AVRO Sink with Time Partitioned File Dataset");
    configurer.addProperty(new Property(SCHEMA, "The schema of the record", true));
    configurer.addProperty(new Property(TPFS_NAME, "Name of the Time Partitioned FileSet Dataset to which the " +
      "records have to be written", true));
    configurer.addProperty(new Property(BASE_PATH, "Optional: The base path for the time partitioned fileset. Should" +
      "be provided if you want the pipeline to create the fileset.", false));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    // if the base path is provided then we should try to create the fileset here
    if (!Strings.isNullOrEmpty(stageConfig.getProperties().get(BASE_PATH))) {
      String tpfsName = stageConfig.getProperties().get(TPFS_NAME);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(tpfsName), "TimePartitionedFileSet name must be given.");
      pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), FileSetProperties.builder()
        .setBasePath(stageConfig.getProperties().get(BASE_PATH))
        .setInputFormat(AvroKeyInputFormat.class)
        .setOutputFormat(AvroKeyOutputFormat.class)
        .setEnableExploreOnCreate(true)
        .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
        .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
        .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
        .setTableProperty("avro.schema.literal", (stageConfig.getProperties().get(SCHEMA)))
        .build());
    }
  }

  @Override
  public void prepareJob(BatchSinkContext context) {
    Map<String, String> sinkArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, context.getLogicalStartTime());
    TimePartitionedFileSet sink = context.getDataset(context.getRuntimeArguments().get(TPFS_NAME), sinkArgs);
    context.setOutput(context.getRuntimeArguments().get(TPFS_NAME), sink);
    Schema avroSchema = new Schema.Parser().parse(context.getRuntimeArguments().get(SCHEMA));
    Job job = context.getHadoopJob();
    AvroJob.setOutputKeySchema(job, avroSchema);
  }

  @Override
  public void transform(GenericRecord input, Emitter<KeyValue<AvroKey<GenericRecord>,
    NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<AvroKey<GenericRecord>, NullWritable>(
      new AvroKey<GenericRecord>(input), NullWritable.get()));
  }
}
