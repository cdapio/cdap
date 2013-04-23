package com.continuuity.flow.queue;

import com.continuuity.test.AppFabricTestBase;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TestFlowQueueIntegration extends AppFabricTestBase {
  private static int MAX_ITERATIONS = 20;

  @Test
  public void testQueuePartition() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestFlowQueueIntegrationApp.class);
    try {
      FlowManager flowManager = applicationManager.startFlow("QueuePartitionFlow");

      StreamWriter s1 = applicationManager.getStreamWriter("s1");
      RuntimeMetrics flowletMetrics1 = RuntimeStats.getFlowletMetrics("TestFlowQueueIntegrationApp",
                                                                      "QueuePartitionFlow",
                                                                      "QueuePartitionTestFlowlet");
      flowManager.setFlowletInstances("QueuePartitionTestFlowlet",
                                      TestFlowQueueIntegrationApp.QueuePartitionTestFlowlet.NUM_INSTANCES);

      RuntimeMetrics flowletMetrics2 = RuntimeStats.getFlowletMetrics("TestFlowQueueIntegrationApp",
                                                                           "QueuePartitionFlow", "QueueBatchTestFlowlet");
      flowManager.setFlowletInstances("QueueBatchTestFlowlet",
                                      TestFlowQueueIntegrationApp.QueueBatchTestFlowlet.NUM_INSTANCES);
      for(int i = 0; i < MAX_ITERATIONS; i++) {
        s1.send(String.valueOf(i));
      }

      flowletMetrics1.waitForProcessed(MAX_ITERATIONS * 3, 10, TimeUnit.SECONDS);
      // TODO: fix RuntimeMetrics collection to handle batch
      flowletMetrics2.waitForProcessed(5, 10, TimeUnit.SECONDS);
    } finally {
      applicationManager.stopAll();
      clearAppFabric();
    }
  }
}
