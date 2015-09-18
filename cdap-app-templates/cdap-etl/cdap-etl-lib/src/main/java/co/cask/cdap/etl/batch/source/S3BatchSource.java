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
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.BatchSource;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads from Amazon S3.
 */
@Plugin(type = "batchsource")
@Name("S3")
@Description("Batch source for Amazon S3")
public class S3BatchSource extends FileBatchSource {
  private static final String ACCESS_ID_DESCRIPTION = "Access ID of the Amazon S3 instance to connect to.";
  private static final String ACCESS_KEY_DESCRIPTION = "Access Key of the Amazon S3 instance to connect to.";

  private final S3BatchConfig config;

  public S3BatchSource(S3BatchConfig config) {
    super(new FileBatchConfig("S3", config.path, config.fileRegex, config.timeTable, config.inputFormatClass,
                              createFileSystemProperties(config.accessID, config.accessKey), config.maxSplitSize));
    this.config = config;
  }

  private static String createFileSystemProperties(String accessID, String accessKey) {
    return new Gson().toJson(ImmutableMap.of(
      "fs.s3n.awsAccessKeyId", accessID,
      "fs.s3n.awsSecretAccessKey", accessKey
    ));
  }

  /**
   * Config class that contains properties needed for the S3 source.
   */
  public static class S3BatchConfig extends PluginConfig {
    @Name("accessID")
    @Description(ACCESS_ID_DESCRIPTION)
    private String accessID;

    @Name("accessKey")
    @Description(ACCESS_KEY_DESCRIPTION)
    private String accessKey;

    @Name("path")
    @Description(PATH_DESCRIPTION)
    private String path;

    @Name("fileRegex")
    @Nullable
    @Description(REGEX_DESCRIPTION)
    private String fileRegex;

    @Name("timeTable")
    @Nullable
    @Description(TABLE_DESCRIPTION)
    private String timeTable;

    @Name("inputFormatClass")
    @Nullable
    @Description(INPUT_FORMAT_CLASS_DESCRIPTION)
    private String inputFormatClass;

    @Name("maxSplitSize")
    @Nullable
    @Description(MAX_SPLIT_SIZE_DESCRIPTION)
    private String maxSplitSize;

    public S3BatchConfig(String accessID, String accessKey, String path, @Nullable String regex,
                         @Nullable String timeTable, @Nullable String inputFormatClass,
                         @Nullable String maxSplitSize) {
      this.accessID = accessID;
      this.accessKey = accessKey;
      this.path = path;
      this.fileRegex = regex;
      this.timeTable = timeTable;
      this.inputFormatClass = inputFormatClass;
      this.maxSplitSize = maxSplitSize;
    }
  }
}
