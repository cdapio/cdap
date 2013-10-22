package com.continuuity.data2.transaction.persist;

import java.io.IOException;
import java.util.List;

/**
 * Represents a log of transaction state changes.
 */
public interface TransactionLog {

  String getName();

  long getTimestamp();

  void append(TransactionEdit edit) throws IOException;

  void append(List<TransactionEdit> edits) throws IOException;

  void close() throws IOException;

  TransactionLogReader getReader() throws IOException;
}
