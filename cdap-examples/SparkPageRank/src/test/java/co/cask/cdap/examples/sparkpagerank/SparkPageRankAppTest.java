/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class SparkPageRankAppTest extends TestBase {

  private static final String URL_1 = "http://example.com/page1";
  private static final String URL_2 = "http://example.com/page2";
  private static final String URL_3 = "http://example.com/page3";

  private static final String RANK = "14";
  private static final String TOTAL_PAGES = "1";

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void test() throws Exception {
    // Deploy the SparkPageRankApp
    ApplicationManager appManager = deployApplication(SparkPageRankApp.class);

    // Send a stream events to the Stream
    StreamManager streamManager = getStreamManager(SparkPageRankApp.BACKLINK_URL_STREAM);
    streamManager.send(Joiner.on(" ").join(URL_1, URL_2));
    streamManager.send(Joiner.on(" ").join(URL_1, URL_3));
    streamManager.send(Joiner.on(" ").join(URL_2, URL_1));
    streamManager.send(Joiner.on(" ").join(URL_3, URL_1));

    // Start service
    ServiceManager serviceManager = appManager.getServiceManager(SparkPageRankApp.SERVICE_HANDLERS)
      .start();

    // Wait for service to start since the Spark program needs it
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);

    // Start the SparkPageRankProgram
    SparkManager sparkManager = appManager.getSparkManager(SparkPageRankApp.PageRankSpark.class.getSimpleName())
      .start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);

    // Run RanksCounter which will count the number of pages for a pr
    MapReduceManager mapReduceManager = appManager.getMapReduceManager(SparkPageRankApp.RanksCounter.class
                                                                         .getSimpleName()).start();
    mapReduceManager.waitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    //Query for rank
    URL url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS),
                      SparkPageRankApp.SparkPageRankServiceHandler.RANKS_PATH);

    HttpRequest request = HttpRequest.post(url)
      .withBody(("{\"" + SparkPageRankApp.SparkPageRankServiceHandler.URL_KEY + "\":\"" + URL_1 + "\"}"))
      .build();
    HttpResponse response = HttpRequests.execute(request);

    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(RANK, response.getResponseBodyAsString());

    // Request total pages for a page rank and verify it
    url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS),
                  SparkPageRankApp.SparkPageRankServiceHandler.TOTAL_PAGES_PATH + "/" + RANK);
    response = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(TOTAL_PAGES, response.getResponseBodyAsString());
  }
}
