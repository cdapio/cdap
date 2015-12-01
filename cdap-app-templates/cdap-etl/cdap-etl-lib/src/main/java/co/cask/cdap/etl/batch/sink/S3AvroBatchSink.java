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
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.etl.common.StructuredToAvroTransformer;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * {@link S3AvroBatchSink} that stores data in avro format to S3.
 */
@Plugin(type = "batchsink")
@Name("S3Avro")
@Description("Batch sink to write to Amazon S3 in Avro format.")
public class S3AvroBatchSink extends S3BatchSink<AvroKey<GenericRecord>, NullWritable> {

  private StructuredToAvroTransformer recordTransformer;
  private final S3AvroSinkConfig config;

  private static final String SCHEMA_DESC = "The Avro schema of the record being written to the sink as a JSON " +
    "object.";

  public S3AvroBatchSink(S3AvroSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  protected OutputFormatProvider createOutputFormatProvider(BatchSinkContext context) {
    return new S3AvroOutputFormatProvider(config, context);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  /**
   * Configuration for the S3AvroSink.
   */
  public static class S3AvroSinkConfig extends S3BatchSinkConfig {

    @Name(Properties.S3BatchSink.SCHEMA)
    @Description(SCHEMA_DESC)
    private String schema;

    @SuppressWarnings("unused")
    public S3AvroSinkConfig() {
      super();
    }

    @SuppressWarnings("unused")
    public S3AvroSinkConfig(String basePath, String schema, String accessID, String accessKey, String pathFormat,
                            String filesystemProperties) {
      super(basePath, accessID, accessKey, pathFormat, filesystemProperties);
      this.schema = schema;
    }
  }

  /**
   * Output format provider that sets avro output format to be use in MapReduce.
   */
  public static class S3AvroOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    public S3AvroOutputFormatProvider(S3AvroSinkConfig config, BatchSinkContext context) {
      @SuppressWarnings("ConstantConditions")
      SimpleDateFormat format = new SimpleDateFormat(config.pathFormat);

      conf = Maps.newHashMap();
      conf.put(JobContext.OUTPUT_KEY_CLASS, AvroKey.class.getName());
      conf.put("avro.schema.output.key", config.schema);
      conf.put(FileOutputFormat.OUTDIR,
               String.format("%s/%s", config.basePath, format.format(context.getLogicalStartTime())));
    }

    @Override
    public String getOutputFormatClassName() {
      return AvroKeyOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
