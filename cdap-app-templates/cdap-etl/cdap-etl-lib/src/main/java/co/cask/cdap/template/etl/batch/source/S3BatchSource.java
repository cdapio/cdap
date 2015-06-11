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
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchContext;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchSource} to use S3 as a Source.
 */
@Plugin(type = "source")
@Name("S3")
@Description("Batch source for S3")
public class S3BatchSource extends BatchSource<LongWritable, Text, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(S3BatchSource.class);

  private S3BatchConfig config;
  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
   Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
   Schema.Field.of("body", Schema.of(Schema.Type.BYTES))
  );

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = context.getHadoopJob();

    job.getConfiguration().set("fs.s3n.awsAccessKeyId", config.accessID);
    job.getConfiguration().set("fs.s3n.awsSecretAccessKey", config.accessKey);

    FileInputFormat.addInputPath(job, new Path(config.path));
  }


  public void transform(KeyValue<LongWritable, Text> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord output = StructuredRecord.builder(DEFAULT_SCHEMA)
      .set("ts", input.getKey().get())
      .set("body", input.getValue())
      .build();
    emitter.emit(output);

  }

  public static class S3BatchConfig extends PluginConfig {

    private String name;

    private String accessID;

    private String accessKey;

    private String path;

  }
}
