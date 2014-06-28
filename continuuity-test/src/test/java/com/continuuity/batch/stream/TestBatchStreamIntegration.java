package com.continuuity.batch.stream;

import com.continuuity.test.ApplicationManager;
import com.continuuity.test.MapReduceManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.StreamWriter;
import com.continuuity.test.XSlowTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(XSlowTests.class)
public class TestBatchStreamIntegration extends ReactorTestBase {
  @Test
  public void testStreamBatch() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestBatchStreamIntegrationApp.class);
    try {
      StreamWriter s1 = applicationManager.getStreamWriter("s1");
      for (int i = 0; i < 50; i++) {
        s1.send(String.valueOf(i));
      }

      MapReduceManager mapReduceManager = applicationManager.startMapReduce("StreamTestBatch");
      mapReduceManager.waitForFinish(60, TimeUnit.SECONDS);

      // TODO: verify results

    } finally {
      applicationManager.stopAll();
      TimeUnit.SECONDS.sleep(1);
      clear();
    }
  }
}
