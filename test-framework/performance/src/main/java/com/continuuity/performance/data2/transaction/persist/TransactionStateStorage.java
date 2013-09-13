package com.continuuity.performance.data2.transaction.persist;

import java.io.IOException;
import java.util.List;

/**
 * Defines the common contract for persisting transaction state changes.
 */
public interface TransactionStateStorage {
  public void append(TransactionEdit edit) throws IOException;

  public void append(List<TransactionEdit> edits) throws IOException;
}
