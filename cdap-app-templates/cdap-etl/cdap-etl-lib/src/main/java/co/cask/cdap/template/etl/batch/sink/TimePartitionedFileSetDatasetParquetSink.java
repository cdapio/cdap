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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.SchemaConverter;
import co.cask.cdap.template.etl.common.StructuredToAvroTransformer;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Job;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Parquet record to {@link TimePartitionedFileSet}
 */
@Plugin(type = "sink")
@Name("TPFSParquet")
@Description("PARQUET Sink with Time Partitioned File Dataset")
public class TimePartitionedFileSetDatasetParquetSink extends
  BatchSink<StructuredRecord, Void, GenericRecord> {

  private static final String SCHEMA_DESC = "The schema of the record";
  private static final String TPFS_NAME_DESC = "Name of the Time Partitioned FileSet Dataset to which the records " +
    "have to be written";
  private static final String BASE_PATH_DESC = "The base path for the time partitioned fileset. Defaults to the " +
    "name of the dataset";
  private final StructuredToAvroTransformer recordTransformer = new StructuredToAvroTransformer();

  /**
   * Config for TimePartitionedFileSetDatasetParquetSink
   */
  public static class TPFSParquetSinkConfig extends PluginConfig {

    @Description(TPFS_NAME_DESC)
    private String name;

    @Description(SCHEMA_DESC)
    private String schema;

    @Description(BASE_PATH_DESC)
    @Nullable
    private String basePath;
  }

  private final TPFSParquetSinkConfig tpfsParquetSinkConfig;
  private String hiveSchema;

  public TimePartitionedFileSetDatasetParquetSink(TPFSParquetSinkConfig tpfsParquetSinkConfig)
    throws IOException, UnsupportedTypeException {
    this.tpfsParquetSinkConfig = tpfsParquetSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {

    String tpfsName = tpfsParquetSinkConfig.name;
    String basePath = tpfsParquetSinkConfig.basePath == null ? tpfsName : tpfsParquetSinkConfig.basePath;
    try {
      hiveSchema = SchemaConverter.toHiveSchema(Schema.parseJson(tpfsParquetSinkConfig.schema.toLowerCase()));
    } catch (UnsupportedTypeException | IOException e) {
      throw new RuntimeException("Error: Schema is not valid ", e);
    }
    pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), FileSetProperties.builder()
      .setBasePath(basePath)
      .setInputFormat(AvroParquetInputFormat.class)
      .setOutputFormat(AvroParquetOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("parquet")
      .setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1))
      .build());
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Map<String, String> sinkArgs = Maps.newHashMap();
    TimePartitionedFileSetArguments.setOutputPartitionTime(sinkArgs, context.getLogicalStartTime());
    TimePartitionedFileSet sink = context.getDataset(tpfsParquetSinkConfig.name, sinkArgs);
    context.setOutput(tpfsParquetSinkConfig.name, sink);
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(
      tpfsParquetSinkConfig.schema.toLowerCase());
    Job job = context.getHadoopJob();
    AvroParquetOutputFormat.setSchema(job, avroSchema);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }
}
