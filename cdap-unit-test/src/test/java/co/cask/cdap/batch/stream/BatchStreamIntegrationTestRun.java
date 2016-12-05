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

package co.cask.cdap.batch.stream;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(XSlowTests.class)
public class BatchStreamIntegrationTestRun extends TestFrameworkTestBase {

  /**
   * TestsMapReduce that consumes from stream using BytesWritableStreamDecoder
   * @throws Exception
   */
  @Test
  public void testStreamBatch() throws Exception {
    submitAndVerifyStreamBatchJob(TestBatchStreamIntegrationApp.class, "s_1", "StreamTestBatch", 300);
  }

  /**
   * Tests MapReduce that consumes from stream using IdentityStreamEventDecoder
   * @throws Exception
   */
  @Test
  public void testStreamBatchIdDecoder() throws Exception {
    submitAndVerifyStreamBatchJob(TestBatchStreamIntegrationApp.class, "s_1", "StreamTestBatchIdDecoder", 300);
  }

  /**
   * Tests MapReduce that consumes from stream without mapper.
   */
  @Test
  public void testNoMapperStreamInput() throws Exception {
    submitAndVerifyStreamBatchJob(NoMapperApp.class, "nomapper", "NoMapperMapReduce", 120);
  }


  /**
   * Tests MapReduce that consumes from stream without mapper.
   */
  @Test
  public void testNoMapperOtherStreamInput() throws Exception {
    submitAndVerifyStreamOtherNamespaceBatchJob(NoMapperStreamSpaceApp.class, NoMapperStreamSpaceApp.INPUTSTREAMSPACE,
                                                "nomapper", "NoMapperMapReduce", 120);
  }

  private void submitAndVerifyStreamBatchJob(Class<? extends AbstractApplication> appClass,
                                             String streamWriter, String mapReduceName, int timeout) throws Exception {
    ApplicationManager applicationManager = deployApplication(appClass);
    StreamManager streamManager = getStreamManager(streamWriter);
    verifyStreamBatchJob(streamManager, applicationManager, mapReduceName, timeout);
  }

  private void submitAndVerifyStreamOtherNamespaceBatchJob(Class<? extends AbstractApplication> appClass,
                                                           String namespace, String streamWriter, String mapReduceName,
                                                           int timeout) throws Exception {
    NamespaceId namespaceId = new NamespaceId(namespace);
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName(namespace).build();
    getNamespaceAdmin().create(namespaceMeta);
    deployApplication(namespaceId, appClass);
    ApplicationManager applicationManager = deployApplication(appClass);
    StreamManager streamManager = getStreamManager(namespaceId.stream(streamWriter));
    verifyStreamBatchJob(streamManager, applicationManager, mapReduceName, timeout);
  }

  private void verifyStreamBatchJob(StreamManager streamManager, ApplicationManager applicationManager,
                                    String mapReduceName, int timeout) throws Exception {
    for (int i = 0; i < 50; i++) {
      streamManager.send(String.valueOf(i));
    }

    MapReduceManager mapReduceManager = applicationManager.getMapReduceManager(mapReduceName).start();
    mapReduceManager.waitForFinish(timeout, TimeUnit.SECONDS);

    // The MR job simply turns every stream event body into key/value pairs, with key==value.
    DataSetManager<KeyValueTable> datasetManager = getDataset("results");
    KeyValueTable results = datasetManager.get();
    for (int i = 0; i < 50; i++) {
      byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
      Assert.assertArrayEquals(key, results.read(key));
    }
  }
}
