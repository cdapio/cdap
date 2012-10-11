package com.payvment.continuuity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.ReadColumnRange;
import com.continuuity.api.data.lib.CounterTable;
import com.continuuity.api.data.lib.SortedCounterTable;
import com.continuuity.api.data.lib.SortedCounterTable.Counter;
import com.continuuity.api.data.util.Bytes;
import com.continuuity.api.data.util.Helpers;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.flow.FlowTestHelper;
import com.continuuity.flow.FlowTestHelper.TestFlowHandle;
import com.continuuity.flow.flowlet.internal.TupleSerializer;
import com.google.gson.Gson;
import com.payvment.continuuity.data.ActivityFeed;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
import com.payvment.continuuity.data.ClusterTable;
import com.payvment.continuuity.data.ProductTable;
import com.payvment.continuuity.entity.ProductFeedEntry;

public class TestClusterWriterFlow extends PayvmentBaseFlowTest {

  private static final String CLEAR_CSV =
      "reset_clusters,50,\"from unit test\"";

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

    // Generate a clear event
//    writeToStream(flowName, streamName, bytes)
    
    // Ensure no clusters in table
    
    // Generate and insert some clusters
    
    
    
    // Verify clusters in table
    
    // Generate a clear event, ensure no clusters in table
    
    
    // If we are here, flow ran successfully!
    assertTrue(FlowTestHelper.stopFlow(flowHandle));

  }
}
