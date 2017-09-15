/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.spark;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.spark.app.SparkAppUsingLocalFiles;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit-tests for testing Spark program.
 */
public class SparkLocalFileAccessTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static final Map<Class<? extends Application>, File> ARTIFACTS = new IdentityHashMap<>();

  @BeforeClass
  public static void init() throws IOException {
    ARTIFACTS.put(SparkAppUsingLocalFiles.class, createArtifactJar(SparkAppUsingLocalFiles.class));
  }

  private ApplicationManager deploy(Class<? extends Application> appClass) throws Exception {
    return deployWithArtifact(appClass, ARTIFACTS.get(appClass));
  }

  @Test
  public void testSparkWithLocalFiles() throws Exception {
    testSparkWithLocalFiles(SparkAppUsingLocalFiles.class,
                            SparkAppUsingLocalFiles.JavaSparkUsingLocalFiles.class.getSimpleName(), "java");
    testSparkWithLocalFiles(SparkAppUsingLocalFiles.class,
                            SparkAppUsingLocalFiles.ScalaSparkUsingLocalFiles.class.getSimpleName(), "scala");
  }

  private void testSparkWithLocalFiles(Class<? extends Application> appClass,
                                       String sparkProgram, String prefix) throws Exception {
    ApplicationManager applicationManager = deploy(appClass);
    URI localFile = createLocalPropertiesFile(prefix);

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram)
      .start(Collections.singletonMap(SparkAppUsingLocalFiles.LOCAL_FILE_RUNTIME_ARG, localFile.toString()));
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 2, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> kvTableManager = getDataset(SparkAppUsingLocalFiles.OUTPUT_DATASET_NAME);
    KeyValueTable kvTable = kvTableManager.get();
    Map<String, String> expected = ImmutableMap.of("a", "1", "b", "2", "c", "3");
    List<byte[]> deleteKeys = new ArrayList<>();
    try (CloseableIterator<KeyValue<byte[], byte[]>> scan = kvTable.scan(null, null)) {
      for (int i = 0; i < 3; i++) {
        KeyValue<byte[], byte[]> next = scan.next();
        Assert.assertEquals(expected.get(Bytes.toString(next.getKey())), Bytes.toString(next.getValue()));
        deleteKeys.add(next.getKey());
      }
      Assert.assertFalse(scan.hasNext());
    }

    // Cleanup after run
    kvTableManager.flush();
    for (byte[] key : deleteKeys) {
      kvTable.delete(key);
    }
    kvTableManager.flush();
  }


  private URI createLocalPropertiesFile(String filePrefix) throws IOException {
    File file = TMP_FOLDER.newFile(filePrefix + "-local.properties");
    try (OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file))) {
      out.write("a=1\n");
      out.write("b = 2\n");
      out.write("c= 3");
    }
    return file.toURI();
  }
}
