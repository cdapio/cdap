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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.common.SchemaConverter;
import co.cask.cdap.etl.common.SnapshotFileSetConfig;
import co.cask.cdap.etl.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import javax.annotation.Nullable;


/**
 * {@link SnapshotFileBatchSink} that stores data in Parquet format.
 */
@Plugin(type = "batchsink")
@Name("SnapshotParquet")
@Description("Sink for a SnapshotFileSet that writes data in Parquet format.")
public class SnapshotFileBatchParquetSink extends SnapshotFileBatchSink<Void, GenericRecord> {
  private final SnapshotParquetConfig config;

  private StructuredToAvroTransformer recordTransformer;

  public SnapshotFileBatchParquetSink(SnapshotParquetConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    // parse to make sure it's valid
    new org.apache.avro.Schema.Parser().parse(config.schema.toLowerCase());
    String hiveSchema;
    try {
      hiveSchema = SchemaConverter.toHiveSchema(Schema.parseJson(config.schema.toLowerCase()));
    } catch (UnsupportedTypeException | IOException e) {
      throw new RuntimeException("Error: Schema is not valid ", e);
    }

    propertiesBuilder
      .setInputFormat(AvroParquetInputFormat.class)
      .setOutputFormat(AvroParquetOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("parquet")
      .setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1))
      .setOutputProperty("parquet.avro.schema", config.schema.toLowerCase());
  }

  /**
   * Config for SnapshotFileBatchParquetSink.
   */
  public static class SnapshotParquetConfig extends SnapshotFileSetConfig {
    @Description("The Parquet schema of the record being written to the Sink as a JSON Object.")
    private String schema;

    public SnapshotParquetConfig(String name, @Nullable String basePath, String schema) {
      super(name, basePath, null);
      this.schema = schema;
    }
  }
}
