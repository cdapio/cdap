package com.continuuity.gateway;

import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;

public class DummyMDS extends MetadataService {
  DummyMDS() {
    super(new NoOperationExecutor());
  }
  @Override
  public Stream getStream(Account account, Stream stream) {
    stream.setExists(true);
    return stream;
  }
}
