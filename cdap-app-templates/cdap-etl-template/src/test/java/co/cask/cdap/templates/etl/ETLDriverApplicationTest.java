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

package co.cask.cdap.templates.etl;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.templates.etl.core.ETLDriverApplication;
import co.cask.cdap.templates.etl.lib.sinks.realtime.KeyValueTableSink;
import co.cask.cdap.templates.etl.lib.sources.realtime.TwitterSource;
import co.cask.cdap.templates.etl.lib.transforms.FilterOutRetweetTransform;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkerManager;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ETLDriverApplicationTest extends TestBase {

  @Test
  public void testRealtimeAdapter() throws Exception {
    ApplicationManager appManager = deployApplication(ETLDriverApplication.class);
    Map<String, String> runtimeArgs = Maps.newHashMap();
    runtimeArgs.put("source", TwitterSource.class.getName());
    runtimeArgs.put("transform", FilterOutRetweetTransform.class.getName());
    runtimeArgs.put("sink", KeyValueTableSink.class.getName());
    runtimeArgs.put("ConsumerKey", "00WdU6Vtsx0gLVQIBQ0yDhfSt");
    runtimeArgs.put("ConsumerSecret", "6CqweBbE7Te3vnUt5LIOXqrwVNaNy7yVE1nGmrXzKwEuSv2pLF");
    runtimeArgs.put("AccessToken", "22606115-JPb52Q15Z1Qwz3y0lpvZ8CnZIhyiwK5mikUJGCIsI");
    runtimeArgs.put("AccessTokenSecret", "lmbI1qy1wTH2N5GVN74Yx3xuVNlYiyL8FEqd2mIccysYm");
    runtimeArgs.put("tableName", "tweetTable");
    WorkerManager workerManager = appManager.startWorker("RealtimeAdapterDriver", runtimeArgs);
    TimeUnit.SECONDS.sleep(60);
    workerManager.stop();
  }

  @Test
  public void testBatchAdapter() throws Exception {
    ApplicationManager appManager = deployApplication(ETLDriverApplication.class);
    Map<String, String> runtimeArgs = Maps.newHashMap();
    runtimeArgs.put("streamName", "batchStream");
    StreamWriter writer = appManager.getStreamWriter("batchStream");
    writer.send("Hello");
    writer.send("World");
    DataSetManager<KeyValueTable> dataSetManager = appManager.getDataSet("KVTableBatchSource");
    KeyValueTable table = dataSetManager.get();
    table.write("Flat", "Tire");
    table.write("James", "Bond");
    dataSetManager.flush();
    MapReduceManager manager = appManager.startMapReduce("BatchAdapterDriver", runtimeArgs);
    manager.waitForFinish(5, TimeUnit.MINUTES);
  }
}
