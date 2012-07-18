package com.continuuity.data.operation.executor.remote;

import java.io.IOException;

public interface OpexClientProvider {

  void initialize() throws IOException;
  OperationExecutorClient getClient();
  void returnClient(OperationExecutorClient client);

}
