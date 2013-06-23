package com.continuuity.gateway;

import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;

/**
 * this is for testing and returns exists=true for all streams that do not
 * start with "x"
 */
public class DummyMDS extends MetadataService {
  DummyMDS() {
    super(new NoOperationExecutor());
  }

  boolean allowAll = false;

  public void allowAll() {
    this.allowAll = true;
  }

  @Override
  public Stream getStream(Account account, Stream stream) {
    if (!allowAll && stream.getId().startsWith("x"))
      stream.setExists(false);
    return stream;
  }
}
