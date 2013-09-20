package com.continuuity.performance.opex;

import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;

/**
 * OpexProvider that uses NoOperationExecutor which always returns empty results.
 */
public class NoOpexProvider extends OpexProvider {

  @Override
  OperationExecutor create() {
    return new NoOperationExecutor();
  }

}
