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

package co.cask.cdap.template.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.common.AvroToStructuredTransformer;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredToAvroTransformer;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.Map;

/**
 *
 */
@Plugin(type = "source")
@Name("TPFSAvro")
@Description("AVRO Source with Time Partitioned File Dataset")
public class TimePartitionedFileSetDatasetAvroSource extends
  BatchReadableSource<LongWritable, AvroKey<GenericRecord>, StructuredRecord> {

  private static final String SCHEMA_DESC = "The avro schema of the record being written to the Sink as a JSON Object";
  private static final String TPFS_NAME_DESC = "Name of the Time Partitioned FileSet Dataset to which the records " +
    "have to be written. If it doesn't exist, it will be created";
  private static final String BASE_PATH_DESC = "Base path for the time partitioned fileset. Defaults to the " +
    "name of the dataset";

  private final AvroToStructuredTransformer recordTransformer = new AvroToStructuredTransformer();

  public static class TPFSAvroSourceConfig extends PluginConfig {
    @Description(TPFS_NAME_DESC)
    private String name;

    @Description(SCHEMA_DESC)
    private String schema;

    @Description(BASE_PATH_DESC)
    @Nullable
    private String basePath;
  }

  private final TPFSAvroSourceConfig tpfsAvroSourceConfig;

  public TimePartitionedFileSetDatasetAvroSource(TPFSAvroSourceConfig tpfsAvroSourceConfig) {
    this.tpfsAvroSourceConfig = tpfsAvroSourceConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    String tpfsName = tpfsAvroSourceConfig.name;
    String basePath = tpfsAvroSourceConfig.basePath == null ? tpfsName : tpfsAvroSourceConfig.basePath;
    pipelineConfigurer.createDataset(tpfsName, TimePartitionedFileSet.class.getName(), FileSetProperties.builder()
      .setBasePath(basePath)
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", (tpfsAvroSourceConfig.schema))
      .build());
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties = Maps.newHashMap(tpfsAvroSourceConfig.getProperties().getProperties());
    properties.put(Properties.BatchReadableWritable.NAME, tpfsAvroSourceConfig.name);
    properties.put(Properties.BatchReadableWritable.TYPE, TimePartitionedFileSet.class.getName());
    return properties;
  }

  /*@Override
  public void transform(AvroKey<GenericRecord> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(new recordTransformer.transform(input));
  }*/



}
