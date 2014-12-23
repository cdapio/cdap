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
package co.cask.cdap.examples.helloworld;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
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
import java.util.concurrent.TimeoutException;

/**
 * Test for {@link HelloWorld}.
 */
public class HelloWorldTest extends TestBase {

  @Test
  public void test() throws TimeoutException, InterruptedException, IOException {
    // Deploy the HelloWorld application
    ApplicationManager appManager = deployApplication(HelloWorld.class);

    // Start WhoFlow
    FlowManager flowManager = appManager.startFlow("WhoFlow");

    // Send stream events to the "who" Stream
    StreamWriter streamWriter = appManager.getStreamWriter("who");
    streamWriter.send("1");
    streamWriter.send("2");
    streamWriter.send("3");
    streamWriter.send("4");
    streamWriter.send("5");

    try {
      // Wait for the last Flowlet processing 5 events, or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("HelloWorld", "WhoFlow", "saver");
      metrics.waitForProcessed(5, 5, TimeUnit.SECONDS);
      Assert.assertTrue(flowManager.isRunning());
    } finally {
      flowManager.stop();
      Assert.assertFalse(flowManager.isRunning());
    }

    // Start Greeting service and use it
    ServiceManager serviceManager = appManager.startService(HelloWorld.Greeting.SERVICE_NAME);

    // Wait service startup
    serviceStatusCheck(serviceManager, true);

    URL url = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS), "greet");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    Assert.assertEquals("Hello 5!", response);

    appManager.stopAll();
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
