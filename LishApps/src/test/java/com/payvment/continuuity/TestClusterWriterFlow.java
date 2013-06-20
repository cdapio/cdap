/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.payvment.continuuity;

import com.continuuity.api.data.OperationException;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.payvment.continuuity.data.ClusterTable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestClusterWriterFlow extends PayvmentBaseFlowTest {

  private static final String CLEAR_CSV =
    "reset_clusters,10,\"from unit test\"";

  @Test(timeout = 20000)
  public void testClusterWriterFlow() throws InterruptedException, IOException, TimeoutException {
    ApplicationManager applicationManager = deployApplication(LishApp.class);


    // Get references to tables
    ClusterTable clusterTable = (ClusterTable) applicationManager.getDataSet(LishApp.CLUSTER_TABLE);

    // Instantiate and start cluster writer feed flow
    applicationManager.startFlow(ClusterWriterFlow.FLOW_NAME);

    // Generate a clear event and wait for it to be processed
    int numParsed = 0;
    int numReset = 0;
    int numWritten = 0;

    StreamWriter s1 = applicationManager.getStreamWriter(LishApp.CLUSTER_STREAM);

    // Write CSV data
    s1.send(CLEAR_CSV.getBytes());

    numParsed++;
    numReset++;

    RuntimeMetrics m1 =
      RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                     ClusterWriterFlow.FLOW_NAME,
                                     ClusterWriterFlow.PARSER_FLOWLET_NAME);

    System.out.println("Waiting for parsing flowlet to process tuple");
    m1.waitForProcessed(numParsed, 5, TimeUnit.SECONDS);

    RuntimeMetrics m2 =
      RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                     ClusterWriterFlow.FLOW_NAME,
                                     ClusterWriterFlow.RESET_FLOWLET_NAME);

    System.out.println("Waiting for reset flowlet to process tuple");
    m2.waitForProcessed(numReset, 5, TimeUnit.SECONDS);

    // Writer flowlet should not have received anything
    RuntimeMetrics m3 =
      RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                     ClusterWriterFlow.FLOW_NAME,
                                     ClusterWriterFlow.WRITER_FLOWLET_NAME);

    // Writer flowlet should not have received anything
    assertEquals(numWritten, m3.getProcessed());

    try {
      // Ensure no clusters in table
      for (int i = 0; i < 10; i++) {
        assertNull(clusterTable.readCluster(i));
      }
    } catch (OperationException e) {
      System.out.println(e.getLocalizedMessage());
    }


    // Generate and insert some clusters
    try {
      writeCluster(s1, 1, "Sports", 0.0001);
      writeCluster(s1, 1, "Kitchen Appliances", 0.321);
      writeCluster(s1, 1, "Televisions", 0.199);
      writeCluster(s1, 2, "Housewares", 0.011);
      writeCluster(s1, 2, "Pottery", 0.0144);
      writeCluster(s1, 3, "Cutlery", 0.011);
      writeCluster(s1, 3, "Knives", 0.0331);
      writeCluster(s1, 3, "Hatchets", 0.041);
      writeCluster(s1, 4, "Swimwear", 0.011);
      writeCluster(s1, 4, "Goggles", 0.41);
      writeCluster(s1, 4, "Surfing Gear", 0.221);
      writeCluster(s1, 4, "Sports", 0.82);

      numParsed += 12;
      numWritten += 12;

    } catch (IOException e) {
      System.out.println(e.getLocalizedMessage());
    }


    m3 = RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                        ClusterWriterFlow.FLOW_NAME,
                                        ClusterWriterFlow.WRITER_FLOWLET_NAME);

    System.out.println("Waiting for writer flowlet to process tuples");
    m3.waitForProcessed(numWritten, 5, TimeUnit.SECONDS);
    Assert.assertEquals(0L, m3.getException());

    // Verify clusters in table
    System.out.println("Verify clusters in table");

    // Cluster 1
    try {
      Map<String, Double> cluster = clusterTable.readCluster(1);

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

    } catch (OperationException e) {
      System.out.println(e.getLocalizedMessage());
    }

    // Generate a clear event, ensure no clusters in table
    s1.send(CLEAR_CSV.getBytes());
    numParsed++;
    numReset++;

    RuntimeMetrics m4 =
      RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                     ClusterWriterFlow.FLOW_NAME,
                                     ClusterWriterFlow.PARSER_FLOWLET_NAME);

    System.out.println("Waiting for parsing flowlet to process tuple");
    Assert.assertEquals(0L, m4.getException());
    m4.waitForProcessed(numParsed, 5, TimeUnit.SECONDS);

    RuntimeMetrics m5 =
      RuntimeStats.getFlowletMetrics(LishApp.APP_NAME,
                                     ClusterWriterFlow.FLOW_NAME,
                                     ClusterWriterFlow.RESET_FLOWLET_NAME);

    System.out.println("Waiting for reset flowlet to process tuple");
    m5.waitForProcessed(numReset, 5, TimeUnit.SECONDS);
    Assert.assertEquals(0L, m5.getException());

    // Try to read clusters, all should be null
    try {
      assertNull(clusterTable.readCluster(1));
      assertNull(clusterTable.readCluster(2));
      assertNull(clusterTable.readCluster(3));
      assertNull(clusterTable.readCluster(4));
      assertNull(clusterTable.readCluster(5));
    } catch (OperationException e) {
      System.out.println(e.getLocalizedMessage());
    }

    // Stop flow
    //assertTrue(FlowTestHelper.stopFlow(flowHandle));
    applicationManager.stopAll();
  }

  private void writeCluster(StreamWriter streamWriter, int cluster, String category, double weight) throws IOException {
    try {
      streamWriter.send(new String("" + cluster + ",\"" + category + "\"," + weight).getBytes());
    } catch (IOException e) {
      throw e;
    }
  }

}
