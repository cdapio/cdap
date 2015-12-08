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

package co.cask.cdap.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.common.AvroToStructuredTransformer;
import co.cask.cdap.etl.common.HiveSchemaConverter;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link BatchSource} to read Avro record from {@link TimePartitionedFileSet}
 */
@Plugin(type = "batchsource")
@Name("TPFSParquet")
@Description("Reads from a TimePartitionedFileSet whose data is in Parquet format.")
public class TimePartitionedFileSetDatasetParquetSource extends
  TimePartitionedFileSetSource<NullWritable, GenericRecord> {
  private final TPFSParquetConfig tpfsParquetConfig;

  private final AvroToStructuredTransformer recordTransformer = new AvroToStructuredTransformer();

  /**
   * Config for TimePartitionedFileSetDatasetParquetSource
   */
  public static class TPFSParquetConfig extends TPFSConfig {

    @Description("The Parquet schema of the record being read from the source as a JSON Object.")
    private String schema;

    @Override
    protected void validate() {
      super.validate();
      try {
        new org.apache.avro.Schema.Parser().parse(schema);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
      }
    }
  }

  public TimePartitionedFileSetDatasetParquetSource(TPFSParquetConfig tpfsParquetConfig) {
    super(tpfsParquetConfig);
    this.tpfsParquetConfig = tpfsParquetConfig;
  }

  @Override
  protected void addFileSetProperties(FileSetProperties.Builder properties) {
    properties.setInputFormat(AvroParquetInputFormat.class)
      .setOutputFormat(AvroParquetOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("parquet");
    try {
      String hiveSchema = HiveSchemaConverter.toHiveSchema(
        co.cask.cdap.api.data.schema.Schema.parseJson(tpfsParquetConfig.schema.toLowerCase()));
      properties.setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1));
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException("schema " + tpfsParquetConfig.schema + " is not supported.", e);
    } catch (Exception e) {
      throw new IllegalArgumentException("schema " + tpfsParquetConfig.schema + " is invalid.", e);
    }
  }

  @Override
  protected void addInputFormatConfiguration(Map<String, String> config) {
    try {
      Job job = Job.getInstance();
      Configuration hConf = job.getConfiguration();
      hConf.clear();

      Schema avroSchema = new Schema.Parser().parse(tpfsParquetConfig.schema.toLowerCase());
      AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);
      for (Map.Entry<String, String> entry : hConf) {
        config.put(entry.getKey(), entry.getValue());
      }
    } catch (IOException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void transform(KeyValue<NullWritable, GenericRecord> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(recordTransformer.transform(input.getValue()));
  }
}
