package com.continuuity.metrics2.collector.server.plugins;

import akka.dispatch.Future;
import akka.dispatch.OnComplete;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.PortDetector;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Tests sending metrics to librato.
 */
public class LibratoMetricsProcessorTest {
  private static int port;
  private static Server server;
  private static LibratoServer libratoServer;

  public static class LibratoServer extends HttpServlet {
    private int count;

    public LibratoServer() {
      count = 0;
    }

    protected void doPost(HttpServletRequest request,
                         HttpServletResponse response)
      throws ServletException, IOException {
      count++;
      response.setContentType("application/json");
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().println("{response : \"ok\"}");
    }

    public int getCount() {
      return count;
    }
  }

  @BeforeClass
  public static void before() throws Exception {
    port = PortDetector.findFreePort();
    server = new Server();
    Connector con = new SelectChannelConnector();
    con.setPort(port);
    server.addConnector(con);
    Context context = new Context(server, "/", Context.SESSIONS);
    libratoServer = new LibratoServer();
    context.addServlet(new ServletHolder(libratoServer), "/v1/metrics");
    server.start();
  }

  @AfterClass
  public static void after() throws Exception {
    if(server != null) {
      server.stop();
    }
  }

  @Test
  public void testSendToLibrato() throws Exception {
    CConfiguration configuration = CConfiguration.create();

    configuration.set("librato.account.name", "nitin@continuuity.com");
    configuration.set(
      "librato.account.token",
      "6b8ac685e4665f78ba1f8f10b0c509b054fb92f96d711fe8f7505225064f81e1"
    );
    String url = String.format("http://localhost:%d/v1/metrics", port);
    configuration.set("librato.url", url);

    MetricsProcessor processor = new LibratoMetricsProcessor(configuration);

    class OnCompleteHandler extends OnComplete<MetricResponse.Status> {
      int success = 0;
      int failed = 0;
      @Override
      public void onComplete(Throwable throwable, MetricResponse.Status
        status) {
        if(throwable != null) {
          failed++;
        } else {
          success++;

        }
      }
      public int getFailed() {
        return failed;
      }
      public int getSuccess() {
        return success;
      }
    }

    OnCompleteHandler handler = new OnCompleteHandler();
    for(int i = 0; i < 10; ++i) {
      MetricRequest request1 = new MetricRequest.Builder(true)
        .setRequestType("System")
        .setMetricName("UNIT_TEST_METRIC.count")
        .setTimestamp(System.currentTimeMillis()/1000)
        .addTag("host", "a.b.c")
        .setValue(i^2).create();
      Future<MetricResponse.Status> response1 = processor.process(request1);
      response1.onComplete(handler);
      while(! response1.isCompleted()) {
        Thread.sleep(1);
      }
      MetricRequest request2 = new MetricRequest.Builder(true)
        .setRequestType("System")
        .setMetricName("UNIT_TEST_METRIC")
        .setTimestamp(System.currentTimeMillis()/1000)
        .addTag("host", "a.b.c")
        .setValue(i^2).create();
      Future<MetricResponse.Status> response2 = processor.process(request2);
      response2.onComplete(handler);
      while(! response2.isCompleted()) {
        Thread.sleep(1);
      }
      Thread.sleep(500);
    }

    Assert.assertTrue(libratoServer.getCount() > 1);
    Assert.assertThat(handler.getFailed(), CoreMatchers.is(0));
    Assert.assertThat(handler.getSuccess(), CoreMatchers.is(20));
  }
}
