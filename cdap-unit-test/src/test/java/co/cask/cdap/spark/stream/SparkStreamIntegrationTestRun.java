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
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
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
    // Test for reading stream cross namespace, reading and writing to dataset cross namespace
    // TestSparkStreamIntegrationApp deployed in default namespace
    // which reads a stream from streamNS and writes to a dataset in its own ns (default)
    // TestSparkCrossNSDatasetApp deployed at crossNSDatasetAppNS:
    //  reading from the dataset in default (created by TestSparkStreamIntegrationApp) and write to a dataset
    // in outputDatasetNS
    NamespaceMeta streamNSMeta = new NamespaceMeta.Builder().setName("streamNS").build();
    NamespaceMeta crossNSDatasetAppNS = new NamespaceMeta.Builder().setName("crossNSDatasetAppNS").build();
    NamespaceMeta outputDatasetNS = new NamespaceMeta.Builder().setName("outputDatasetNS").build();
    getNamespaceAdmin().create(streamNSMeta);
    getNamespaceAdmin().create(crossNSDatasetAppNS);
    getNamespaceAdmin().create(outputDatasetNS);
    addDatasetInstance(outputDatasetNS.getNamespaceId().dataset("finalDataset"), "keyValueTable");

    StreamManager streamManager = getStreamManager(streamNSMeta.getNamespaceId().stream("testStream"));
    streamManager.createStream();
    for (int i = 0; i < 50; i++) {
      streamManager.send(String.valueOf(i));
    }

    // deploy TestSparkStreamIntegrationApp in default namespace
    ApplicationManager spark1 = deployApplication(TestSparkStreamIntegrationApp.class);

    Map<String, String> args = ImmutableMap.of(
      TestSparkStreamIntegrationApp.SparkStreamProgram.INPUT_STREAM_NAMESPACE,
      streamNSMeta.getNamespaceId().getNamespace(),
      TestSparkStreamIntegrationApp.SparkStreamProgram.INPUT_STREAM_NAME,
      "testStream"
    );

    SparkManager sparkManager = spark1.getSparkManager("SparkStreamProgram").start(args);
    sparkManager.waitForFinish(120, TimeUnit.SECONDS);
    // Verify the results written in default namespace by spark1
    DataSetManager<KeyValueTable> datasetManager = getDataset("result");
    verifyDatasetResult(datasetManager);

    // deploy the cross  ns dataset app in datasetNS namespace
    ApplicationManager spark2 = deployApplication(crossNSDatasetAppNS.getNamespaceId(),
                                                  TestSparkCrossNSDatasetApp.class);
    args = ImmutableMap.of(
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.INPUT_DATASET_NAMESPACE,
      NamespaceId.DEFAULT.getNamespace(),
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.INPUT_DATASET_NAME, "result",
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.OUTPUT_DATASET_NAMESPACE,
      outputDatasetNS.getNamespaceId().getNamespace(),
      TestSparkCrossNSDatasetApp.SparkCrossNSDatasetProgram.OUTPUT_DATASET_NAME, "finalDataset"
    );

    sparkManager = spark2.getSparkManager("SparkCrossNSDatasetProgram").start(args);
    sparkManager.waitForFinish(120, TimeUnit.SECONDS);
    // Verify the results written in DEFAULT by spark2
    datasetManager = getDataset(outputDatasetNS.getNamespaceId().dataset("finalDataset"));
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
