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
  private static final int FLOWLET_TIMEOUT = 30;
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
      for(int i = 0; i < TestFlowQueueIntegrationApp.MAX_ITERATIONS; i++) {
        s1.send(String.valueOf(i));
      }

      flowletMetrics1.waitForProcessed(TestFlowQueueIntegrationApp.MAX_ITERATIONS * 3,
                                       FLOWLET_TIMEOUT, TimeUnit.SECONDS);
      flowletMetrics2.waitForProcessed(TestFlowQueueIntegrationApp.QueueBatchTestFlowlet.NUM_INSTANCES,
                                       FLOWLET_TIMEOUT, TimeUnit.SECONDS);

      // Wait for Fifo consumers to finish
      TimeUnit.SECONDS.sleep(1);
    } finally {
      applicationManager.stopAll();
      clearAppFabric();
    }
  }
}
