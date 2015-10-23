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
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.Properties;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link S3BatchSink} that stores the data of the latest run of an ETL Pipeline in S3.
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class S3BatchSink<KEY_OUT, VAL_OUT> extends FileBatchSink<KEY_OUT, VAL_OUT> {

  private static final String ACCESS_ID_DESCRIPTION = "Access ID of the Amazon S3 instance to connect to.";
  private static final String ACCESS_KEY_DESCRIPTION = "Access Key of the Amazon S3 instance to connect to.";

  private final S3BatchSinkConfig config;
  protected S3BatchSink(S3BatchSinkConfig config) {
    super(config);
    this.config = config;
    // update fileSystemProperties to include accessID and accessKey, so prepareRun can only set fileSystemProperties
    // in configuration, and not deal with accessID and accessKey separately
    this.config.fileSystemProperties = updateFileSystemProperties(this.config.fileSystemProperties,
                                                                  this.config.accessID, this.config.accessKey);
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    if (config.fileSystemProperties != null) {
      Map<String, String> properties = GSON.fromJson(config.fileSystemProperties, MAP_STRING_STRING_TYPE);
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
  }

  private static String updateFileSystemProperties(@Nullable String fileSystemProperties, String accessID,
                                                   String accessKey) {
    Map<String, String> providedProperties;
    if (fileSystemProperties == null) {
      providedProperties = new HashMap<>();
    } else {
      providedProperties = GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
    providedProperties.put("fs.s3n.awsAccessKeyId", accessID);
    providedProperties.put("fs.s3n.awsSecretAccessKey", accessKey);
    return GSON.toJson(providedProperties);
  }

  @VisibleForTesting
  S3BatchSinkConfig getConfig() {
    return config;
  }

  /**
   * S3 Sink configuration.
   */
  public static class S3BatchSinkConfig extends FileBatchSink.FileBatchConfig {

    @Name(Properties.S3.ACCESS_ID)
    @Description(ACCESS_ID_DESCRIPTION)
    protected String accessID;

    @Name(Properties.S3.ACCESS_KEY)
    @Description(ACCESS_KEY_DESCRIPTION)
    protected String accessKey;

    public S3BatchSinkConfig() {
      // Set default value for Nullable properties.
      super();
      this.fileSystemProperties = updateFileSystemProperties(null, accessID, accessKey);
    }

    public S3BatchSinkConfig(String basePath, @Nullable String pathFormat,
                             @Nullable String fileSystemProperties, @Nullable String outputField,
                             String accessID, String accessKey) {
      super(basePath, pathFormat, updateFileSystemProperties(fileSystemProperties, accessID, accessKey), outputField);
      this.accessID = accessID;
      this.accessKey = accessKey;
    }
  }
}
