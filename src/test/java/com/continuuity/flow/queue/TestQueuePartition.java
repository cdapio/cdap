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
public class TestQueuePartition extends AppFabricTestBase {
  private static int MAX_ITERATIONS = 20;

  @Test
  public void testQueuePartition() throws Exception {
    ApplicationManager applicationManager = deployApplication(TestQueuePartitionApp.class);
    try {
      FlowManager flowManager = applicationManager.startFlow("QueuePartitionFlow");

      StreamWriter s1 = applicationManager.getStreamWriter("s1");
      RuntimeMetrics roundRobinMetrics = RuntimeStats.getFlowletMetrics("TestQueuePartitionApp", "QueuePartitionFlow",
                                                                      "RoundRobinFlowlet");
      flowManager.setFlowletInstances("RoundRobinFlowlet", TestQueuePartitionApp.RR_NUM_INSTANCES);

      RuntimeMetrics hashPartitionMetrics = RuntimeStats.getFlowletMetrics("TestQueuePartitionApp",
                                                                           "QueuePartitionFlow", "HashPartitionFlowlet");
      flowManager.setFlowletInstances("HashPartitionFlowlet", TestQueuePartitionApp.HASH_NUM_INSTANCES);
      for(int i = 0; i < MAX_ITERATIONS; i++) {
        s1.send(String.valueOf(i));
      }

      roundRobinMetrics.waitForProcessed(MAX_ITERATIONS, 10, TimeUnit.SECONDS);
      hashPartitionMetrics.waitForProcessed(MAX_ITERATIONS, 10, TimeUnit.SECONDS);
    } finally {
      applicationManager.stopAll();
      clearAppFabric();
    }
  }
}
