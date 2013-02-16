package com.continuuity.internal.app.runtime;

import com.continuuity.data.BatchCollector;
import com.continuuity.data.operation.WriteOperation;

import java.util.List;

/**
 *
 */
public interface ManagedBatchCollector extends BatchCollector {

  List<WriteOperation> capture();

  void reset();
}
