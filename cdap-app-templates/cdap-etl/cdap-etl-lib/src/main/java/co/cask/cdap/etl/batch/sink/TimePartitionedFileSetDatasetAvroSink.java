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

package co.cask.cdap.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.common.StructuredToAvroTransformer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Avro record to {@link TimePartitionedFileSet}
 */
@Plugin(type = "batchsink")
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
    super.configurePipeline(pipelineConfigurer);
    String tpfsName = tpfsSinkConfig.name;
    String basePath = tpfsSinkConfig.basePath == null ? tpfsName : tpfsSinkConfig.basePath;
    // parse it to make sure its valid
    new Schema.Parser().parse(config.schema);
    pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), FileSetProperties.builder()
      .setBasePath(basePath)
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", config.schema)
      .build());
  }

  @Override
  protected Map<String, String> getAdditionalTPFSArguments() {
    Map<String, String> args = new HashMap<>();
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + "avro.schema.output.key", config.schema);
    return args;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
    InvalidEntry entry = new InvalidEntry(100, "dummy", input);
    emitter.emitError(entry);
  }

  /**
   * Config for TimePartitionedFileSetAvroSink
   */
  public static class TPFSAvroSinkConfig extends TPFSSinkConfig {

    @Description(SCHEMA_DESC)
    private String schema;

    public TPFSAvroSinkConfig(String name, String schema, @Nullable String basePath, @Nullable String pathFormat,
                              @Nullable String timeZone) {
      super(name, basePath, pathFormat, timeZone);
      this.schema = schema;
    }
  }

}
