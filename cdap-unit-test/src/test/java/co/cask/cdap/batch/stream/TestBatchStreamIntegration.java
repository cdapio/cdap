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

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Charsets;
import com.google.common.reflect.TypeToken;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(XSlowTests.class)
public class TestBatchStreamIntegration extends TestBase {
  @Test
  public void testStreamBatch() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestBatchStreamIntegrationApp.class);
    try {
      StreamWriter writer = applicationManager.getStreamWriter("s_1");
      for (int i = 0; i < 50; i++) {
        writer.send(String.valueOf(i));
      }

      MapReduceManager mapReduceManager = applicationManager.startMapReduce("StreamTestBatch");
      mapReduceManager.waitForFinish(2, TimeUnit.MINUTES);

      // The MR job simply turns every stream event body into key/value pairs, with key==value.
      DataSetManager<KeyValueTable> datasetManager = applicationManager.getDataSet("results");
      KeyValueTable results = datasetManager.get();
      for (int i = 0; i < 50; i++) {
        byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
        Assert.assertArrayEquals(key, results.read(key));
      }
    } finally {
      applicationManager.stopAll();
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }

  /**
   * Tests MapReduce that consumes from stream without mapper.
   */
  @Test
  public void testNoMapperStreamInput() throws Exception {
    ApplicationManager applicationManager = deployApplication(NoMapperApp.class);
    try {
      StreamWriter writer = applicationManager.getStreamWriter("nomapper");
      for (int i = 0; i < 50; i++) {
        writer.send(String.valueOf(i));
      }

      MapReduceManager mapReduceManager = applicationManager.startMapReduce("NoMapperMapReduce");
      mapReduceManager.waitForFinish(2, TimeUnit.MINUTES);

      // The Reducer in the MR simply turns every stream event body into key/value pairs, with key==value.
      DataSetManager<KeyValueTable> datasetManager = applicationManager.getDataSet("results");
      KeyValueTable results = datasetManager.get();
      for (int i = 0; i < 50; i++) {
        byte[] key = String.valueOf(i).getBytes(Charsets.UTF_8);
        Assert.assertArrayEquals(key, results.read(key));
      }

    } finally {
      applicationManager.stopAll();
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }
}
