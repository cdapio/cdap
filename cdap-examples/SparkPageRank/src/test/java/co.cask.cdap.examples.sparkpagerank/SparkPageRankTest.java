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
package co.cask.cdap.examples.sparkpagerank;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ProcedureClient;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SparkPageRankTest extends TestBase {

  private static final String URL_PAIR12 = "http://example.com/page1 http://example.com/page2";
  private static final String URL_PAIR13 = "http://example.com/page1 http://example.com/page3";
  private static final String URL_PAIR21 = "http://example.com/page2 http://example.com/page1";
  private static final String URL_PAIR31 = "http://example.com/page3 http://example.com/page1";
  private static final String RANK = "\"1.3690036520596678\"";

  @Test(expected = IOException.class)
  public void test() throws Exception {
    ApplicationManager appManager = deployApplication(SparkPageRankApp.class);
    FlowManager flowManager = appManager.startFlow("BackLinkFlow");
    try {
      sendData(appManager);
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("SparkPageRank", "BackLinkFlow", "reader");
      metrics.waitForProcessed(4, 5, TimeUnit.SECONDS);
      SparkManager sparkManager = appManager.startSpark("SparkPageRankProgram");
      sparkManager.waitForFinish(60, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
    }
    verifyRankProcedure(appManager);
  }

  private void sendData(ApplicationManager appManager) throws IOException {
    StreamWriter streamWriter = appManager.getStreamWriter("backlinkURLStream");
    streamWriter.send(URL_PAIR12);
    streamWriter.send(URL_PAIR13);
    streamWriter.send(URL_PAIR21);
    streamWriter.send(URL_PAIR31);
  }

  private void verifyRankProcedure(ApplicationManager appManager) throws IOException {
    ProcedureManager procedureManager = appManager.startProcedure("RanksProcedure");
    ProcedureClient client = procedureManager.getClient();
    String response = client.query("rank", ImmutableMap.of("url", "http://example.com/page1"));
    Assert.assertEquals(RANK, response);
    response = client.query("rank", ImmutableMap.of("URL", "http://example.com/page1"));
    Assert.assertEquals(RANK, response);
    client.query("rank", ImmutableMap.of("wrong_argument", "http://example.com/page1"));
    client.query("rank", ImmutableMap.of("url", "a page which does not exist"));
  }

}
