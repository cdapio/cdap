package com.continuuity.data.operation;

import java.util.Comparator;

/**
 * Comparator of {@link WriteOperation}s that orders them in ascending
 * priority order (lower numbers order first).
 */
public class WriteOperationComparator implements Comparator<WriteOperation> {

  @Override
  public int compare(WriteOperation left, WriteOperation right) {
    if (left.getPriority() == right.getPriority()) {
      return 0;
    }
    if (left.getPriority() < right.getPriority()) {
      return -1;
    }
    return 1;
  }

}
