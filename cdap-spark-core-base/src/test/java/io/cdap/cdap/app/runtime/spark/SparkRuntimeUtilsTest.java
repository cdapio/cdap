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

package io.cdap.cdap.app.runtime.spark;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Properties;
import java.util.zip.ZipFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link SparkRuntimeUtils}.
 */
public class SparkRuntimeUtilsTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @After
  public void resetSparkRuntimeEnvStopped() throws Exception {
    Field stoppedField = SparkRuntimeEnv$.MODULE$.getClass().getDeclaredField("stopped");
    stoppedField.setAccessible(true);
    stoppedField.setBoolean(SparkRuntimeEnv$.MODULE$, false);
  }

  @Test
  public void testCreateConfArchive() throws IOException {
    File confDir = TEMP_FOLDER.newFolder();
    File confFile = new File(confDir, "testing.conf");
    Files.write("Testing Message", confFile, Charsets.UTF_8);

    SparkConf conf = new SparkConf();
    conf.set("testing", "value");

    File archiveFile = SparkRuntimeUtils.createConfArchive(conf, "test.properties",
                                                           confDir.getAbsolutePath(),
                                                           TEMP_FOLDER.newFile());

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

  @Test
  public void testInitSparkMainCompletedAndCompletedWithException() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(cConf, new Configuration());
    SparkClassLoader sparkClassLoader = new SparkClassLoader(runtimeContext);
    ClassLoader previous = ClassLoaders.setContextClassLoader(sparkClassLoader);
    try {
      SparkProgramCompletion completion = SparkRuntimeUtils.initSparkMain();
      completion.completed();
      Assert.assertSame(sparkClassLoader, Thread.currentThread().getContextClassLoader());
    } finally {
      ClassLoaders.setContextClassLoader(previous);
    }

    resetSparkRuntimeEnvStopped();

    SparkRuntimeContext runtimeContext2 = SparkRuntimeContextTest.createRuntimeContext(cConf, new Configuration());
    SparkClassLoader sparkClassLoader2 = new SparkClassLoader(runtimeContext2);
    ClassLoader previous2 = ClassLoaders.setContextClassLoader(sparkClassLoader2);
    try {
      SparkProgramCompletion completion2 = SparkRuntimeUtils.initSparkMain();
      completion2.completedWithException(new IllegalStateException("user spark failure"));
      Assert.assertSame(sparkClassLoader2, Thread.currentThread().getContextClassLoader());
    } finally {
      ClassLoaders.setContextClassLoader(previous2);
    }
  }
}
