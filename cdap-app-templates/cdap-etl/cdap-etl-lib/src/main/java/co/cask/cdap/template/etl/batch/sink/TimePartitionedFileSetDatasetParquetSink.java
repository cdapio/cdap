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
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.SchemaConverter;
import co.cask.cdap.template.etl.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Job;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;

/**
 * A {@link BatchSink} to write Parquet records to a {@link TimePartitionedFileSet}.
 */
@Plugin(type = "sink")
@Name("TPFSParquet")
@Description("Parquet Sink with Time Partitioned File Dataset.")
public class TimePartitionedFileSetDatasetParquetSink extends
  TimePartitionedFileSetDatasetSink<Void, GenericRecord> {

  private final StructuredToAvroTransformer recordTransformer = new StructuredToAvroTransformer();
  private String hiveSchema;

  public TimePartitionedFileSetDatasetParquetSink(TPFSSinkConfig tpfsSinkConfig) {
    super(tpfsSinkConfig);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    String tpfsName = tpfsSinkConfig.name;
    String basePath = tpfsSinkConfig.basePath == null ? tpfsName : tpfsSinkConfig.basePath;
    try {
      hiveSchema = SchemaConverter.toHiveSchema(Schema.parseJson(tpfsSinkConfig.schema.toLowerCase()));
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
    super.prepareRun(context);
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(
      tpfsSinkConfig.schema.toLowerCase());
    Job job = context.getHadoopJob();
    AvroParquetOutputFormat.setSchema(job, avroSchema);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }
}
