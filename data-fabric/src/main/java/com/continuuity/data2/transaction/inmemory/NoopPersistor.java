package com.continuuity.data2.transaction.inmemory;

import com.google.common.util.concurrent.AbstractIdleService;

import java.io.IOException;

/**
 * A persistor implementation that does nothing. Used for testing.
 */
public class NoopPersistor extends AbstractIdleService implements StatePersistor {

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
  }

  @Override
  public void persist(String tag, byte[] state) throws IOException {
  }

  @Override
  public void delete(String tag) throws IOException {
  }

  @Override
  public byte[] readBack(String tag) throws IOException {
    return null;
  }
}
