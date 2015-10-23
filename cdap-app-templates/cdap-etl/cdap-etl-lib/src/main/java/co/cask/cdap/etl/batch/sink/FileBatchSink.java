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
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.Properties;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.lang.reflect.Type;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Sink that stores the output of the latest run of an ETL Pipeline on a Distributed File System.
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class FileBatchSink<KEY_OUT, VAL_OUT> extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  private static final String PATH_DESC = "The S3 path where the data is stored. Example: 's3n://logs'.";
  private static final String PATH_FORMAT_DESCRIPTION = "The format for the path that will be suffixed to the " +
    "basePath; for example: the format 'yyyy-MM-dd-HH-mm' will create a file path ending in '2015-01-01-20-42'. " +
    "Default format used is 'yyyy-MM-dd-HH-mm'.";
  private static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties " +
    "needed for the distributed file system. Defaults to an empty map";
  private static final String OUTPUT_FIELD_DESCRIPTION = "The name of the field in the input Structured record " +
    "that should be emitted from the sink. Defaults to 'data'";
  private static final String DEFAULT_PATH_FORMAT = "yyyy-MM-dd-HH-mm";
  private static final String DEFAULT_OUTPUT_FIELD = "data";
  protected static final Gson GSON = new Gson();
  protected static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final FileBatchConfig config;

  protected FileBatchSink(FileBatchConfig config) {
    this.config = config;
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

  /**
   * File Sink configuration.
   */
  public static class FileBatchConfig extends PluginConfig {

    @Name(Properties.FileSink.BASE_PATH)
    @Description(PATH_DESC)
    public String basePath;

    @Name(Properties.FileSink.PATH_FORMAT)
    @Description(PATH_FORMAT_DESCRIPTION)
    @Nullable
    public String pathFormat;

    @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
    @Nullable
    public String fileSystemProperties;

    @Description(OUTPUT_FIELD_DESCRIPTION)
    @Nullable
    public String outputField;

    public FileBatchConfig() {
      // Set default value for Nullable properties.
      this.pathFormat = DEFAULT_PATH_FORMAT;
      this.fileSystemProperties = GSON.toJson(ImmutableMap.<String, String>of());
      this.outputField = DEFAULT_OUTPUT_FIELD;
    }

    public FileBatchConfig(String basePath, @Nullable String pathFormat,
                           @Nullable String fileSystemProperties, @Nullable String outputField) {
      this.basePath = basePath;
      this.pathFormat = Strings.isNullOrEmpty(pathFormat) ? DEFAULT_PATH_FORMAT : pathFormat;
      this.fileSystemProperties = fileSystemProperties;
      this.outputField = Strings.isNullOrEmpty(outputField) ? DEFAULT_OUTPUT_FIELD : outputField;
    }
  }
}
