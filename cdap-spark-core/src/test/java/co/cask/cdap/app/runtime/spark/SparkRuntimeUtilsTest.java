/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.zip.ZipFile;

/**
 * Unit tests for {@link SparkRuntimeUtils}.
 */
public class SparkRuntimeUtilsTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCreateConfArchive() throws IOException {
    File confDir = TEMP_FOLDER.newFolder();
    File confFile = new File(confDir, "testing.conf");
    Files.write("Testing Message", confFile, Charsets.UTF_8);

    SparkConf conf = new SparkConf();
    conf.set("testing", "value");

    File archiveFile = SparkRuntimeUtils.createConfArchive(conf, "test.properties",
                                                           confDir.getAbsolutePath(),
                                                           TEMP_FOLDER.newFile().getAbsolutePath());

    try (ZipFile zipFile = new ZipFile(archiveFile)) {
      Properties properties = new Properties();
      try (InputStream is = zipFile.getInputStream(zipFile.getEntry("test.properties"))) {
        properties.load(is);
        Assert.assertEquals("value", properties.getProperty("testing"));
      }

      try (InputStream is = zipFile.getInputStream(zipFile.getEntry("testing.conf"))) {
        Assert.assertEquals("Testing Message", Bytes.toString(ByteStreams.toByteArray(is)));
      }
    }
  }
}
