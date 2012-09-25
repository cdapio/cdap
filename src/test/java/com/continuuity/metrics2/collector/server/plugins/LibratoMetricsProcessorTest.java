package com.continuuity.metrics2.collector.server.plugins;

import akka.dispatch.Future;
import akka.dispatch.OnComplete;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import com.continuuity.metrics2.collector.MetricType;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests sending metrics to librato.
 */
public class LibratoMetricsProcessorTest {

  @Test
  public void testSendToLibrato() throws Exception {
    CConfiguration configuration = CConfiguration.create();

    configuration.set("librato.account.name", "nitin@continuuity.com");
    configuration.set(
      "librato.account.token",
      "6b8ac685e4665f78ba1f8f10b0c509b054fb92f96d711fe8f7505225064f81e1"
    );
    configuration.set(
      "librato.url",
      "https://metrics-api.librato.com/v1/metrics"
    );

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
      Thread.sleep(1000);
    }

    Thread.sleep(10000);
    Assert.assertThat(handler.getFailed(), CoreMatchers.is(0));
    Assert.assertThat(handler.getSuccess(), CoreMatchers.is(20));
  }
}
