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

package co.cask.cdap.examples.loganalysis;

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
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Unit test for {@link LogAnalysisApp}
 */
public class LogAnalysisAppTest extends TestBase {

  private static final Gson GSON = new Gson();
  private static final String LOG_1 = "127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] \"GET /home.html HTTP/1.1\" 200 2048";
  private static final String LOG_2 = "127.0.1.1 - - [21/Jul/2014:9:55:27 -0800] \"GET /home.html HTTP/1.1\" 400 2048";
  private static final String LOG_3 = "127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] \"GET /index.html HTTP/1.1\" 200 2048";

  private static final String TOTAL_HITS_VALUE = "2";
  private static final String TOTAL_RESPONSE_VALUE = "2";
  private static final String RESPONSE_CODE = "200";
  private static final Map<String, Integer> TPFS_RESULT = ImmutableMap.of("127.0.1.1", 1, "127.0.0.1", 2);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void test() throws Exception {
    // Deploy the App
    ApplicationManager appManager = deployApplication(LogAnalysisApp.class);

    // Send a stream events to the Stream
    StreamManager streamManager = getStreamManager(LogAnalysisApp.LOG_STREAM);
    streamManager.send(LOG_1);
    streamManager.send(LOG_2);
    streamManager.send(LOG_3);

    // run the spark program
    SparkManager sparkManager = appManager.getSparkManager(
      LogAnalysisApp.ResponseCounterSpark.class.getSimpleName()).start();
    sparkManager.waitForRun(ProgramRunStatus.COMPLETED, 60, TimeUnit.SECONDS);

    // run the mapreduce job
    MapReduceManager mapReduceManager = appManager.getMapReduceManager(HitCounterProgram.class.getSimpleName()).start();
    mapReduceManager.waitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    // start and wait for services
    ServiceManager hitCounterServiceManager = getServiceManager(appManager, LogAnalysisApp.HIT_COUNTER_SERVICE);
    ServiceManager responseCounterServiceManager = getServiceManager(appManager,
                                                                     LogAnalysisApp.RESPONSE_COUNTER_SERVICE);
    ServiceManager requestCounterServiceManager = getServiceManager(appManager,
                                                                    LogAnalysisApp.REQUEST_COUNTER_SERVICE);

    //Query for hit counts and verify it
    URL totalHitsURL = new URL(hitCounterServiceManager.getServiceURL(15, TimeUnit.SECONDS),
                               LogAnalysisApp.HitCounterServiceHandler.HIT_COUNTER_SERVICE_PATH);

    HttpResponse response = HttpRequests.execute(HttpRequest.post(totalHitsURL)
                                                   .withBody("{\"url\":\"" + "/home.html" + "\"}")
                                                   .build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertEquals(TOTAL_HITS_VALUE, response.getResponseBodyAsString());

    // query for total responses for a response code and verify it
    URL responseCodeURL = new URL(responseCounterServiceManager.getServiceURL(15, TimeUnit.SECONDS),
                                  LogAnalysisApp.ResponseCounterHandler.RESPONSE_COUNT_PATH
                                    + "/" + RESPONSE_CODE);
    HttpRequest request = HttpRequest.get(responseCodeURL).build();
    response = HttpRequests.execute(request);
    Assert.assertEquals(TOTAL_RESPONSE_VALUE, response.getResponseBodyAsString());

    // query to get partitions in the request count tpfs
    URL requestCountFilsetsURL = new URL(requestCounterServiceManager.getServiceURL(15, TimeUnit.SECONDS),
                                         LogAnalysisApp.RequestCounterHandler.REQUEST_COUNTER_PARTITIONS_PATH);
    request = HttpRequest.get(requestCountFilsetsURL).build();
    response = HttpRequests.execute(request);
    TreeSet<String> partitions = GSON.fromJson(response.getResponseBodyAsString(),
                                           new TypeToken<TreeSet<String>>() {
                                           }.getType());
    Assert.assertEquals(1, partitions.size());
    String partition = partitions.iterator().next();

    //Query for the contents of the files in this partition and verify
    URL requestFilesetContentURL = new URL(requestCounterServiceManager.getServiceURL(15, TimeUnit.SECONDS),
                                           LogAnalysisApp.RequestCounterHandler.REQUEST_FILE_CONTENT_PATH);

    response = HttpRequests.execute(HttpRequest.post(requestFilesetContentURL)
                                      .withBody("{\"" +
                                                  LogAnalysisApp.RequestCounterHandler.REQUEST_FILE_PATH_HANDLER_KEY +
                                                  "\":\"" + partition + "\"}")
                                      .build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Map<String, Integer> responseMap = GSON.fromJson(response.getResponseBodyAsString(),
                                                     new TypeToken<Map<String, Integer>>() { }.getType());
    Assert.assertTrue(responseMap.equals(TPFS_RESULT));
  }

  private ServiceManager getServiceManager(ApplicationManager appManager, String serviceName)
    throws InterruptedException, TimeoutException, ExecutionException {
    // Start the service
    ServiceManager serviceManager =
      appManager.getServiceManager(serviceName).start();

    // Wait for service startup
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    return serviceManager;
  }
}
