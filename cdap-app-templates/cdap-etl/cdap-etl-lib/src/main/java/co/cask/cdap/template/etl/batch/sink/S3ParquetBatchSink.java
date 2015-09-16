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
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredToAvroTransformer;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.mapreduce.Job;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.util.Map;

/**
 * {@link S3ParquetBatchSink} that stores data in parquet format to S3.
 */
@Plugin(type = "batchsink")
@Name("S3Parquet")
@Description("Sink for a S3 that writes data in Parquet format.")
public class S3ParquetBatchSink extends S3BatchSink<Void, GenericRecord> {

  private StructuredToAvroTransformer recordTransformer;
  private final S3ParquetSinkConfig config;

  private static final String SCHEMA_DESC = "The Avro schema of the record being written to the Sink as a JSON " +
    "Object.";

  public S3ParquetBatchSink(S3ParquetSinkConfig config) {
    super(config);
    this.config = config;
  }


  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    super.prepareRun(context);
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(config.schema.toLowerCase());
    Job job = context.getHadoopJob();
    AvroParquetOutputFormat.setSchema(job, avroSchema);
    context.addOutput(config.name, new S3ParquetOutputFormatProvider(config));
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }

  /**
   * Configuration for the S3ParquetConfig.
   */
  public static class S3ParquetSinkConfig extends S3BatchSinkConfig {

    @Name(Properties.S3BatchSink.SCHEMA)
    @Description(SCHEMA_DESC)
    private String schema;

    public S3ParquetSinkConfig(String name, String basePath, String schema,
                               String accessID, String accessKey) {
      super(name, basePath, accessID, accessKey);
      this.schema = schema;
    }
  }

  /**
   * Output format provider that sets parquet output format to be use in MapReduce.
   */
  public static class S3ParquetOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public S3ParquetOutputFormatProvider(S3ParquetSinkConfig config) {
      conf = Maps.newHashMap();
      conf.put("input.format", AvroParquetInputFormat.class.getName());
      conf.put("output.format", AvroParquetOutputFormat.class.getName());
    }

    @Override
    public String getOutputFormatClassName() {
      return AvroParquetOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
