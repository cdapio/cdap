package com.payvment.continuuity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Test;

import com.continuuity.api.data.OperationException;
import com.continuuity.flow.FlowTestHelper;
import com.continuuity.flow.FlowTestHelper.TestFlowHandle;
import com.payvment.continuuity.data.ClusterTable;

public class TestClusterWriterFlow extends PayvmentBaseFlowTest {

  private static final String CLEAR_CSV =
      "reset_clusters,10,\"from unit test\"";

  @Test(timeout = 20000)
  public void testClusterWriterFlow() throws Exception {
    // Get references to tables
    ClusterTable clusterTable =
        new ClusterTable(getDataFabric(), getRegistry());

    // Instantiate cluster writer feed flow
    ClusterWriterFlow clusterWriterFlow = new ClusterWriterFlow();

    // Start the flow
    TestFlowHandle flowHandle = startFlow(clusterWriterFlow);
    assertTrue(flowHandle.isSuccess());

    // Generate a clear event and wait for it to be processed
    int numParsed = ClusterWriterFlow.ClusterSourceParser.numProcessed;
    int numReset = ClusterWriterFlow.ClusterReset.numProcessed;
    int numWritten = ClusterWriterFlow.ClusterWriter.numProcessed;
    writeToStream(ClusterWriterFlow.inputStream, CLEAR_CSV.getBytes());
    numParsed++;
    numReset++;
    while (ClusterWriterFlow.ClusterSourceParser.numProcessed < numParsed) {
      System.out.println("Waiting for parsing flowlet to process tuple");
      Thread.sleep(500);
    }
    while (ClusterWriterFlow.ClusterReset.numProcessed < numReset) {
      System.out.println("Waiting for reset flowlet to process tuple");
      Thread.sleep(500);
    }
    
    // Writer flowlet should not have received anything
    assertEquals(numWritten, ClusterWriterFlow.ClusterWriter.numProcessed);
    
    // Ensure no clusters in table
    for (int i=0; i<10; i++) {
      assertNull(clusterTable.readCluster(i));
    }
    
    // Generate and insert some clusters
    writeCluster(1, "Sports", 0.0001);
    writeCluster(1, "Kitchen Appliances", 0.321);
    writeCluster(1, "Televisions", 0.199);
    writeCluster(2, "Housewares", 0.011);
    writeCluster(2, "Pottery", 0.0144);
    writeCluster(3, "Cutlery", 0.011);
    writeCluster(3, "Knives", 0.0331);
    writeCluster(3, "Hatchets", 0.041);
    writeCluster(4, "Swimwear", 0.011);
    writeCluster(4, "Goggles", 0.41);
    writeCluster(4, "Surfing Gear", 0.221);
    writeCluster(4, "Sports", 0.82);
    numParsed += 12;
    numWritten += 12;
    
    // Wait for them to be written
    while (ClusterWriterFlow.ClusterWriter.numProcessed < numWritten) {
      System.out.println("Waiting for writer flowlet to process tuples");
      Thread.sleep(500);
    }
    
    // Verify clusters in table
    
    // Cluster 1
    Map<String,Double> cluster = clusterTable.readCluster(1);
    assertNotNull(cluster);
    assertEquals(3, cluster.size());
    assertTrue(cluster.containsKey("Sports"));
    assertTrue(cluster.containsKey("Kitchen Appliances"));
    assertTrue(cluster.containsKey("Televisions"));
    
    // Cluster 2
    cluster = clusterTable.readCluster(2);
    assertNotNull(cluster);
    assertEquals(2, cluster.size());
    assertTrue(cluster.containsKey("Housewares"));
    assertTrue(cluster.containsKey("Pottery"));
    
    // Cluster 3
    cluster = clusterTable.readCluster(3);
    assertNotNull(cluster);
    assertEquals(3, cluster.size());
    assertTrue(cluster.containsKey("Cutlery"));
    assertTrue(cluster.containsKey("Knives"));
    assertTrue(cluster.containsKey("Hatchets"));
    
    // Cluster 4
    cluster = clusterTable.readCluster(4);
    assertNotNull(cluster);
    assertEquals(4, cluster.size());
    assertTrue(cluster.containsKey("Swimwear"));
    assertTrue(cluster.containsKey("Goggles"));
    assertTrue(cluster.containsKey("Surfing Gear"));
    assertTrue(cluster.containsKey("Sports"));

    // Cluster 5 should not exist
    assertNull(clusterTable.readCluster(5));
    
    // Generate a clear event, ensure no clusters in table
    writeToStream(ClusterWriterFlow.inputStream, CLEAR_CSV.getBytes());
    numParsed++;
    numReset++;
    while (ClusterWriterFlow.ClusterSourceParser.numProcessed < numParsed) {
      System.out.println("Waiting for parsing flowlet to process tuple");
      Thread.sleep(500);
    }
    while (ClusterWriterFlow.ClusterReset.numProcessed < numReset) {
      System.out.println("Waiting for reset flowlet to process tuple");
      Thread.sleep(500);
    }
    
    // Stop flow
    assertTrue(FlowTestHelper.stopFlow(flowHandle));
  }

  private void writeCluster(int cluster, String category, double weight)
      throws OperationException {
    writeToStream(ClusterWriterFlow.inputStream,
        new String("" + cluster + ",\"" + category + "\"," + weight).getBytes());
  }

}
