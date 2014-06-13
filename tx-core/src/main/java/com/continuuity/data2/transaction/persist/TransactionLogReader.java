package com.continuuity.data2.transaction.persist;

import java.io.Closeable;
import java.io.IOException;

/**
 * Represents a reader for {@link TransactionLog} instances.
 */
public interface TransactionLogReader extends Closeable {
  /**
   * Returns the next {@code TransactionEdit} from the log file, based on the current position, or {@code null}
   * if the end of the file has been reached.
   */
  TransactionEdit next() throws IOException;

  /**
   * Populates {@code reuse} with the next {@code TransactionEdit}, based on the reader's current position in the
   * log file.
   * @param reuse The {@code TransactionEdit} instance to populate with the log entry data.
   * @return The {@code TransactionEdit} instance, or {@code null} if the end of the file has been reached.
   * @throws IOException If an error is encountered reading the log data.
   */
  TransactionEdit next(TransactionEdit reuse) throws IOException;
}
