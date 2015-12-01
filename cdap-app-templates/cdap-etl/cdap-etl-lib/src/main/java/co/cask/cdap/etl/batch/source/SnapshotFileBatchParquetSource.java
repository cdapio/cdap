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
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.common.AvroToStructuredTransformer;
import co.cask.cdap.etl.common.SchemaConverter;
import co.cask.cdap.etl.common.SnapshotFileSetConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.NullWritable;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import javax.annotation.Nullable;

/**
 * Reads data written by a {@link SnapshotFileBatchParquetSource}. Reads only the most recent partition.
 */
@Plugin(type = "batchsource")
@Name("SnapshotParquet")
@Description("Reads the most recent snapshot that was written to a SnapshotParquet sink.")
public class SnapshotFileBatchParquetSource extends SnapshotFileBatchSource<NullWritable, GenericRecord> {
  private final AvroToStructuredTransformer recordTransformer = new AvroToStructuredTransformer();
  private final SnapshotParquetConfig config;

  public SnapshotFileBatchParquetSource(SnapshotParquetConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void transform(KeyValue<NullWritable, GenericRecord> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(recordTransformer.transform(input.getValue()));
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    propertiesBuilder.setInputFormat(AvroParquetInputFormat.class)
      .setOutputFormat(AvroParquetOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("parquet");
    try {
      String hiveSchema = SchemaConverter.toHiveSchema(
        co.cask.cdap.api.data.schema.Schema.parseJson(config.schema.toLowerCase()));
      propertiesBuilder.setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1));
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException("schema " + config.schema + " is not supported.", e);
    } catch (Exception e) {
      throw new IllegalArgumentException("schema " + config.schema + " is invalid.", e);
    }
  }

  /**
   * Config for SnapshotFileBatchAvroSource
   */
  public static class SnapshotParquetConfig extends SnapshotFileSetConfig {
    @Description("The Parquet schema of the records to read.")
    private String schema;

    public SnapshotParquetConfig(String name, @Nullable String basePath, String schema) {
      super(name, basePath, null);
      this.schema = schema;
    }
  }
}
