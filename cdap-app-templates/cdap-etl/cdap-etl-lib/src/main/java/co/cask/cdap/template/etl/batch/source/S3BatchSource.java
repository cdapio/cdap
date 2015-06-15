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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchContext;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

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
    Schema.Field.of("key", Schema.of(Schema.Type.STRING)),
   Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );

  public S3BatchSource (S3BatchConfig config) {
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.getConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAIGO2GTK4V7XRUHWQ");
    job.getConfiguration().set("fs.s3n.awsSecretAccessKey", "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");
    FileInputFormat.setInputPathFilter(job, S3Filter.class);
    FileInputFormat.addInputPath(job, new Path("s3n://caskalytics/some_logs/"));
  }


  @Override
  public void transform(KeyValue<LongWritable, Text> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord output = StructuredRecord.builder(DEFAULT_SCHEMA)
      .set("key", String.valueOf(System.currentTimeMillis()))
      .set("body", input.getValue().toString())
      .build();
    emitter.emit(output);
  }

  /**
   * fucking javadoc checkstyle
   */
  public class S3Filter extends Configured implements PathFilter {

    @Override
    public boolean accept(Path path) {
      Date prevHour = new Date();
      prevHour.setTime(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
      String currentTime = new SimpleDateFormat("yyyy-MM-dd-HH").format(prevHour);
      currentTime = "2015-01-25-07";

      String filename = path.toString();
      if (filename.equals("s3n://caskalytics/some_logs") || filename.contains(currentTime)) {
        LOG.info("true");
        return true;
      }
      LOG.info("false");
      return false;

      //return true;
    }
  }


  /**
   * insert javadoc here
   */
  public static class S3BatchConfig extends PluginConfig {

    @Name("name")
    @Description("name lel")
    private String name;

    /*@Name("accessID")
    @Description("Access Id for s3")
    private String accessID;

    @Name("accessKey")
    @Description("Access Key for s3")
    private String accessKey;

    @Name("path")
    @Description("Path to files to be read")
    private String path;*/

    public S3BatchConfig(String name/*, String accessID, String accessKey, String path*/) {
      this.name = name;
      /*this.accessID = accessID;
      this.accessKey = accessKey;
      this.path = path;*/
    }

  }
}
