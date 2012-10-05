package com.continuuity.metrics2.collector.plugins;

import com.continuuity.common.utils.PortDetector;
import com.continuuity.metrics2.collector.server.OpenTSDBInMemoryServer;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Testing of opentsdb client.
 */
public class OpenTSDBClientTest {
  private static ExecutorService executorService
    = Executors.newFixedThreadPool(2);
  private static int tsdbPort;
  private static OpenTSDBInMemoryServer server;

  @BeforeClass
  public static void beforeClass() throws Exception {
    tsdbPort = PortDetector.findFreePort();
    server = new OpenTSDBInMemoryServer(tsdbPort);
    executorService.submit(server);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if(server != null) {
      server.stop();
    }
    Thread.sleep(200); // sleep to make sure server shuts down correctly.
    executorService.shutdown();
  }

  /**
   * Test OpenTSDBClient.
   */
  @Test
  public void testTSDBClientConnect() throws Exception {
    OpenTSDBClient client = new OpenTSDBClient("localhost", tsdbPort);

    client.send("put continuuity.loadavg.1m 1288946927 0.36 host=foo")
      .awaitUninterruptibly();

    Thread.sleep(200);

    // Check on the server side if it has received the command.
    Assert.assertThat(server.getCommandCount(), CoreMatchers.is(1));
  }
}
