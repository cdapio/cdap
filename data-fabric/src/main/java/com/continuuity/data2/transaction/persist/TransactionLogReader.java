package com.continuuity.data2.transaction.persist;

import java.io.IOException;
import java.util.Iterator;

/**
 * Represents a reader for {@link TransactionLog} instances.
 */
public interface TransactionLogReader {
  TransactionEdit next() throws IOException;

  TransactionEdit next(TransactionEdit reuse) throws IOException;

  void close() throws IOException;
}
