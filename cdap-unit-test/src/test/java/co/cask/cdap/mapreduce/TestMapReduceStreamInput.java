/*
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

package co.cask.cdap.mapreduce;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Test reading from a stream with map reduce.
 */
public class TestMapReduceStreamInput extends TestBase {

  @Test
  public void test() throws Exception {

    ApplicationManager applicationManager = deployApplication(AppWithMapReduceUsingStream.class);
    StreamWriter streamWriter = applicationManager.getStreamWriter("mrStream");
    streamWriter.send("hello world");
    streamWriter.send("foo bar");

    try {
      MapReduceManager mrManager = applicationManager.startMapReduce("BodyTracker");
      mrManager.waitForFinish(180, TimeUnit.SECONDS);

      KeyValueTable latestDS = (KeyValueTable) getDataset("latest").get();
      Assert.assertNotNull(latestDS.read("hello world"));
      Assert.assertNotNull(latestDS.read("foo bar"));
    } finally {
      applicationManager.stopAll();
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }
}
