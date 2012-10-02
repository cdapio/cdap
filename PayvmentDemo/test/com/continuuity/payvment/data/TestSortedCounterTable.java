package com.continuuity.payvment.data;

import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.continuuity.api.data.BatchCollectionClient;
import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.operation.executor.OperationExecutor;

public class TestSortedCounterTable {

  @SuppressWarnings("unused")
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    OperationExecutor opex = null;
    DataFabric dataFabric = new DataFabricImpl(opex,
        new OperationContext("testAcct", "testApp"));
    FlowletContext flowletContext = new FlowletContext() {
      @Override
      public void register(BatchCollectionClient client) {}

      @Override
      public DataFabric getDataFabric() {
        
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public int getInstanceId() {
        return 0;
      }
      
    };
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void test() {
    fail("Not yet implemented");
  }

}
