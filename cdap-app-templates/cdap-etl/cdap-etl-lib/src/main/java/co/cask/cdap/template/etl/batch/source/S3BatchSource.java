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

import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchContext;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * A {@link BatchSource} to use S3 as a Source.
 */
public class S3BatchSource extends BatchSource {

  @Override
  public void prepareRun(BatchContext context) throws Exception {
    Job job = context.getHadoopJob();
    //job.getConfiguration().set("fs.s3n.awsAccessKeyId", context.getRuntimeArguments().get("awsAccessKey"));
    //job.getConfiguration().set("fs.s3n.awsSecretAccessKey", context.getRuntimeArguments().get("awsSecretKey"));

    FileInputFormat.addInputPath(job, new Path("path"));


  }

  @Override
  public void initialize(Object context) throws Exception {

  }

  @Override
  public void transform(Object input, Emitter emitter) throws Exception {

  }


  public static class S3BatchConfig extends PluginConfig {

  }
}
