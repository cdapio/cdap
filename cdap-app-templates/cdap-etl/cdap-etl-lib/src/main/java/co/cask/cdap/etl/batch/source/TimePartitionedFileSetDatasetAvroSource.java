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
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.common.AvroToStructuredTransformer;
import com.google.common.base.Throwables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Map;

/**
 * A {@link BatchSource} to read Avro record from {@link TimePartitionedFileSet}
 */
@Plugin(type = "batchsource")
@Name("TPFSAvro")
@Description("Reads from a TimePartitionedFileSet whose data is in Avro format.")
public class TimePartitionedFileSetDatasetAvroSource extends
  TimePartitionedFileSetSource<AvroKey<GenericRecord>, NullWritable> {
  private final TPFSAvroConfig tpfsAvroConfig;

  private final AvroToStructuredTransformer recordTransformer = new AvroToStructuredTransformer();

  /**
   * Config for TimePartitionedFileSetDatasetAvroSource
   */
  public static class TPFSAvroConfig extends TPFSConfig {

    @Description("The Avro schema of the record being read from the source as a JSON Object.")
    private String schema;

    @Override
    protected void validate() {
      super.validate();
      try {
        new Schema.Parser().parse(schema);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse schema with error: " + e.getMessage(), e);
      }
    }
  }

  public TimePartitionedFileSetDatasetAvroSource(TPFSAvroConfig tpfsAvroConfig) {
    super(tpfsAvroConfig);
    this.tpfsAvroConfig = tpfsAvroConfig;
  }

  @Override
  protected void addFileSetProperties(FileSetProperties.Builder properties) {
    properties.setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", (tpfsAvroConfig.schema));
  }

  @Override
  protected void addInputFormatConfiguration(Map<String, String> config) {
    try {
      Job job = Job.getInstance();
      Configuration hConf = job.getConfiguration();
      hConf.clear();

      Schema avroSchema = new Schema.Parser().parse(tpfsAvroConfig.schema);
      AvroJob.setInputKeySchema(job, avroSchema);
      for (Map.Entry<String, String> entry : hConf) {
        config.put(entry.getKey(), entry.getValue());
      }
    } catch (IOException e) {
      // Shouldn't happen
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void transform(KeyValue<AvroKey<GenericRecord>, NullWritable> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(recordTransformer.transform(input.getKey().datum()));
  }
}
