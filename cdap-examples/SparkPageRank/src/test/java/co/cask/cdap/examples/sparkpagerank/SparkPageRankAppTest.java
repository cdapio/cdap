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
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class SparkPageRankAppTest extends TestBase {

  private static final String URL_PAIR12 = "http://example.com/page1 http://example.com/page2";
  private static final String URL_PAIR13 = "http://example.com/page1 http://example.com/page3";
  private static final String URL_PAIR21 = "http://example.com/page2 http://example.com/page1";
  private static final String URL_PAIR31 = "http://example.com/page3 http://example.com/page1";

  private static final String RANK = "14";

  @Test
  public void test() throws Exception {
    // Deploy the SparkPageRankApp
    ApplicationManager appManager = deployApplication(SparkPageRankApp.class);

    // Send a stream events to the Stream
    StreamWriter streamWriter = appManager.getStreamWriter("backlinkURLStream");
    streamWriter.send(URL_PAIR12);
    streamWriter.send(URL_PAIR13);
    streamWriter.send(URL_PAIR21);
    streamWriter.send(URL_PAIR31);

    // Start GoogleTypePR
    ServiceManager transformServiceManager = appManager.startService(SparkPageRankApp.GOOGLE_TYPE_PR_SERVICE_NAME);
    // Wait service startup
    serviceStatusCheck(transformServiceManager, true);

    // Start the SparkPageRankProgram
    SparkManager sparkManager = appManager.startSpark("SparkPageRankProgram");
    sparkManager.waitForFinish(60, TimeUnit.SECONDS);

    // Start CentersService
    ServiceManager serviceManager = appManager.startService(SparkPageRankApp.RANKS_SERVICE_NAME);
    // Wait service startup
    serviceStatusCheck(serviceManager, true);

    String response = requestService(new URL(serviceManager.getServiceURL(5, TimeUnit.SECONDS),
                                             "rank?url=http://example.com/page1"));
    Assert.assertEquals(RANK, response);

    appManager.stopAll();
  }

  private String requestService(URL url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    try {
      return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }
}
