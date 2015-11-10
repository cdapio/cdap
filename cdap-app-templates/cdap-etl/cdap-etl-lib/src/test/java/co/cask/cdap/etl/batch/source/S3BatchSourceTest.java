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

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Tests for {@link S3BatchSource} configuration.
 */
public class S3BatchSourceTest {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @Test
  public void testFileSystemProperties() {
    String accessID = "accessID";
    String accessKey = "accessKey";
    String path = "/path";
    // Test default properties
    S3BatchSource.S3BatchConfig s3BatchConfig = new S3BatchSource.S3BatchConfig(accessID, accessKey, path);
    S3BatchSource s3BatchSource = new S3BatchSource(s3BatchConfig);
    FileBatchSource.FileBatchConfig fileBatchConfig = s3BatchSource.getConfig();
    Map<String, String> fsProperties = GSON.fromJson(fileBatchConfig.fileSystemProperties, MAP_STRING_STRING_TYPE);
    Assert.assertNotNull(fsProperties);
    Assert.assertEquals(2, fsProperties.size());
    Assert.assertEquals(accessID, fsProperties.get("fs.s3n.awsAccessKeyId"));
    Assert.assertEquals(accessKey, fsProperties.get("fs.s3n.awsSecretAccessKey"));

    // Test extra properties
    s3BatchConfig = new S3BatchSource.S3BatchConfig(accessID, accessKey, path, null, null, null,
                                                    GSON.toJson(ImmutableMap.of("s3.compression", "gzip")), null);
    s3BatchSource = new S3BatchSource(s3BatchConfig);
    fileBatchConfig = s3BatchSource.getConfig();
    fsProperties = GSON.fromJson(fileBatchConfig.fileSystemProperties, MAP_STRING_STRING_TYPE);
    Assert.assertNotNull(fsProperties);
    Assert.assertEquals(3, fsProperties.size());
    Assert.assertEquals(accessID, fsProperties.get("fs.s3n.awsAccessKeyId"));
    Assert.assertEquals(accessKey, fsProperties.get("fs.s3n.awsSecretAccessKey"));
    Assert.assertEquals("gzip", fsProperties.get("s3.compression"));
  }
}
