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
      RuntimeMetrics flowletMetrics1 = RuntimeStats.getFlowletMetrics("TestQueuePartitionApp", "QueuePartitionFlow",
                                                                      "Flowlet1");
      flowManager.setFlowletInstances("Flowlet1", TestQueuePartitionApp.Flowlet1.NUM_INSTANCES);

      RuntimeMetrics flowletMetrics2 = RuntimeStats.getFlowletMetrics("TestQueuePartitionApp",
                                                                           "QueuePartitionFlow", "Flowlet2");
      flowManager.setFlowletInstances("Flowlet2", TestQueuePartitionApp.Flowlet2.NUM_INSTANCES);
      for(int i = 0; i < MAX_ITERATIONS; i++) {
        s1.send(String.valueOf(i));
      }

      flowletMetrics1.waitForProcessed(MAX_ITERATIONS * 3, 10, TimeUnit.SECONDS);
      flowletMetrics2.waitForProcessed(MAX_ITERATIONS, 10, TimeUnit.SECONDS);
    } finally {
      applicationManager.stopAll();
      clearAppFabric();
    }
  }
}
