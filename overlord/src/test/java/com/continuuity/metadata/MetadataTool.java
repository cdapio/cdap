package com.continuuity.metadata;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.remote.RemoteOperationExecutor;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;

import java.util.List;

// this is an ad-hoc tool to inspect the meta data (we do not have another
// command line client as of now for the meta data. This should be developed
// into a full-flexed command client for meta data. -Andreas

// at this point it takes ZK quorum to find Opex and starts a MetadataService
// also takes an app and a flow name, and runs various methods against them
// usage: <MetadataTool <zkquorum> <app> <flow>

public class MetadataTool {

  public static void main(String[] args) throws Exception {
    CConfiguration config = CConfiguration.create();
    config.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, args[0]);
    OperationExecutor opex = new RemoteOperationExecutor(config);
    MetadataService service = new MetadataService(opex);

    String accountId = args[1];
    String app = args[2];
    String flow = args[3];

    List<Dataset> datasets = service.getDatasetsByApplication(accountId, app);
    System.out.println("getDatasetsByApp(" + app + "):");
    for (Dataset dataset : datasets) {
      System.out.println("  " + dataset);
    }

    System.out.println("getFlow(" + app + ", " + flow + "):");
    System.out.println("  " +
        service.getFlow(accountId, app, flow));

    System.out.println("getFlowsByApp(" + app + "):");
    List<Flow> flows = service.getFlowsByApplication(accountId, app);
    for (Flow fl : flows) {
      System.out.println("  " + fl);
    }

    System.out.println("getFlows():");
    flows = service.getFlows(accountId);
    for (Flow fl : flows) {
      System.out.println("  " + fl);
    }
  }
}
