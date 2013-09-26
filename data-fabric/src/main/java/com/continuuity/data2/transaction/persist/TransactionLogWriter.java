package com.continuuity.data2.transaction.persist;

import java.io.IOException;

/**
 * Common interface for transaction log writers used by classes extending {@link AbstractTransactionLog}.
 */
public interface TransactionLogWriter {
  /**
   * Adds a new transaction entry to the log.  Note that this does not guarantee that the entry has been flushed
   * to persistent storage until {@link #sync()} has been called.
   *
   * @param entry The transaction edit to append.
   * @throws IOException If an error occurs while writing the edit to storage.
   */
  void append(AbstractTransactionLog.Entry entry) throws IOException;

  /**
   * Syncs any pending transaction edits added through {@link #append(AbstractTransactionLog.Entry)},
   * but not yet flushed to durable storage.
   *
   * @throws IOException If an error occurs while flushing the outstanding edits.
   */
  void sync() throws IOException;

  /**
   * Frees any resources in use by the log implementation.  After {@code close()} is called, any further calls
   * to {@link #append(AbstractTransactionLog.Entry)} or {@link #sync()} will throw an {@link IOException}.
   *
   * @throws IOException
   */
  void close() throws IOException;
}
