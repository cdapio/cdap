/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.spark.stream;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 * Test Spark program integration with Streams
 */
@Category(XSlowTests.class)
public class SparkStreamIntegrationTestRun extends TestFrameworkTestBase {

  @Test
  public void testSparkWithStream() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestSparkStreamIntegrationApp.class);
    StreamManager streamManager = getStreamManager("testStream");
    for (int i = 0; i < 50; i++) {
      streamManager.send(String.valueOf(i));
    }

    SparkManager sparkManager = applicationManager.getSparkManager("SparkStreamProgram").start();
    sparkManager.waitForFinish(120, TimeUnit.SECONDS);

    // The Spark job simply turns every stream event body into key/value pairs, with key==value.
    DataSetManager<KeyValueTable> datasetManager = getDataset("result");
    verifyDatasetResult(datasetManager);
  }

  @Test
  public void testSparkCrossNS() throws Exception {
    // A series of test for cross namespace access here:
    // Spark1 deployed at dataSpace:
    //  reading a stream from streamSpace and write into dataset in the same ns (dataSpace)
    // Spark2 deployed at DEFAULT:
    //  reading from the dataset from dataSpace (created by spark1) and write into dataset in DEFAULT
    getNamespaceAdmin().create(new NamespaceMeta.Builder()
                                 .setName(TestSparkCrossNSStreamApp.SOURCE_STREAM_NAMESPACE).build());
    getNamespaceAdmin().create(new NamespaceMeta.Builder()
                                 .setName(TestSparkCrossNSDatasetApp.SOURCE_DATA_NAMESPACE).build());

    NamespaceId streamSpace = new NamespaceId(TestSparkCrossNSStreamApp.SOURCE_STREAM_NAMESPACE);
    NamespaceId dataSpace = new NamespaceId(TestSparkCrossNSDatasetApp.SOURCE_DATA_NAMESPACE);

    StreamManager streamManager = getStreamManager(streamSpace.toId(), "testStream");
    streamManager.createStream();

    ApplicationManager spark1 = deployApplication(dataSpace.toId(), TestSparkCrossNSStreamApp.class);
    for (int i = 0; i < 50; i++) {
      streamManager.send(String.valueOf(i));
    }

    SparkManager sparkManager = spark1.getSparkManager("SparkStreamProgram").start();
    sparkManager.waitForFinish(120, TimeUnit.SECONDS);
    // Verify the results written in dataSpace by spark1
    DataSetManager<KeyValueTable> datasetManager = getDataset(dataSpace.toId(), "result");
    verifyDatasetResult(datasetManager);

    ApplicationManager spark2 = deployApplication(TestSparkCrossNSDatasetApp.class);
    sparkManager = spark2.getSparkManager("SparkStreamProgram").start();
    sparkManager.waitForFinish(120, TimeUnit.SECONDS);
    // Verify the results written in DEFAULT by spark2
    datasetManager = getDataset("result");
    verifyDatasetResult(datasetManager);
  }

  private void verifyDatasetResult(DataSetManager<KeyValueTable> datasetManager) {
    KeyValueTable results = datasetManager.get();
    for (int i = 0; i < 50; i++) {
      byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
      Assert.assertArrayEquals(key, results.read(key));
    }
  }
}
