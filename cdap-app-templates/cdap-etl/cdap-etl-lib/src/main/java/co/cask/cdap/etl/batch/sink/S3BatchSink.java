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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import javax.annotation.Nullable;

/**
 * {@link S3BatchSink} that stores the data of the latest run of an adapter in S3.
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class S3BatchSink<KEY_OUT, VAL_OUT> extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {

  public static final String PATH_DESC = "The path where the data will be recorded. " +
    "Defaults to the name of the dataset";
  private static final String ACCESS_ID_DESCRIPTION = "Access ID of the Amazon S3 instance to connect to.";
  private static final String ACCESS_KEY_DESCRIPTION = "Access Key of the Amazon S3 instance to connect to.";
  private static final String PATH_FORMAT_DESCRIPTION = "The format for the path that will be suffixed with the " +
    "basePath; for example yyyy-MM-dd-HH-mm will create a file path ending in the format of 2015-01-01-20-42. " +
    "Default format used is yyyy-MM-dd-HH-mm.";
  private static final String DEFAULT_PATH_FORMAT = "yyyy-MM-dd-HH-mm";

  private final S3BatchSinkConfig config;
  protected S3BatchSink(S3BatchSinkConfig config) {
    this.config = config;
  }


  @Override
  public void prepareRun(BatchSinkContext context) {
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    conf.set("fs.s3n.awsAccessKeyId", this.config.accessID);
    conf.set("fs.s3n.awsSecretAccessKey", this.config.accessKey);
  }

  /**
   * S3 Sink configuration.
   */
  public static class S3BatchSinkConfig extends PluginConfig {


    @Name(Properties.S3BatchSink.BASE_PATH)
    @Description(PATH_DESC)
    protected String basePath;

    @Name(Properties.S3.ACCESS_ID)
    @Description(ACCESS_ID_DESCRIPTION)
    protected String accessID;

    @Name(Properties.S3.ACCESS_KEY)
    @Description(ACCESS_KEY_DESCRIPTION)
    protected String accessKey;

    @Name(Properties.S3BatchSink.PATH_FORMAT)
    @Description(PATH_FORMAT_DESCRIPTION)
    @Nullable
    protected String pathFormat;


    public S3BatchSinkConfig(String basePath, String pathFormat,
                              String accessID, String accessKey) {
      this.basePath = basePath;
      this.pathFormat = pathFormat == null || pathFormat.isEmpty() ? DEFAULT_PATH_FORMAT : pathFormat;
      this.accessID = accessID;
      this.accessKey = accessKey;
    }
  }
}
