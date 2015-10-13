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
import co.cask.cdap.etl.api.batch.BatchSource;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads from Amazon S3.
 */
@Plugin(type = "batchsource")
@Name("S3")
@Description("Batch source to use Amazon S3 as a source.")
public class S3BatchSource extends FileBatchSource {
  private static final String ACCESS_ID_DESCRIPTION = "Access ID of the Amazon S3 instance to connect to.";
  private static final String ACCESS_KEY_DESCRIPTION = "Access Key of the Amazon S3 instance to connect to.";
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @SuppressWarnings("unused")
  private final S3BatchConfig config;

  public S3BatchSource(S3BatchConfig config) {
    // update fileSystemProperties with S3 properties, so FileBatchSource.prepareRun can use them
    super(new FileBatchConfig(config.path, config.fileRegex, config.timeTable, config.inputFormatClass,
                              updateFileSystemProperties(
                                config.fileSystemProperties, config.accessID, config.accessKey
                              ),
                              config.maxSplitSize));
    this.config = config;
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

  /**
   * Config class that contains properties needed for the S3 source.
   */
  public static class S3BatchConfig extends FileBatchConfig {
    @Description(ACCESS_ID_DESCRIPTION)
    private String accessID;

    @Description(ACCESS_KEY_DESCRIPTION)
    private String accessKey;

    public S3BatchConfig(String accessID, String accessKey, String path) {
      this(accessID, accessKey, path, null, null, null, null, null);
    }

    public S3BatchConfig(String accessID, String accessKey, String path, @Nullable String regex,
                         @Nullable String timeTable, @Nullable String inputFormatClass,
                         @Nullable String fileSystemProperties, @Nullable String maxSplitSize) {
      super(path, regex, timeTable, inputFormatClass,
            updateFileSystemProperties(fileSystemProperties, accessID, accessKey), maxSplitSize);
      this.accessID = accessID;
      this.accessKey = accessKey;
    }
  }
}
