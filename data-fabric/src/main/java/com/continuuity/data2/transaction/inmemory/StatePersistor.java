package com.continuuity.data2.transaction.inmemory;

import java.io.IOException;

/**
 * A persistence agent for the transaction manager. It can be used to persist all or part of the state,
 * and to restore that state when needed.
 */
public interface StatePersistor {
  public void persist(String tag, byte[] state) throws IOException;
  public void delete(String tag) throws IOException;
  public byte[] readBack(String tag) throws IOException;

  void close();

  void start();
}
