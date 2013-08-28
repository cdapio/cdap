package com.continuuity.data2.transaction.queue;

import com.continuuity.common.utils.ImmutablePair;

import java.io.IOException;
import java.util.Map;

/**
 * Interface for queue scan operation.
 */
public interface QueueScanner {

  public ImmutablePair<byte[], Map<byte[], byte[]>> next() throws IOException;

  public void close() throws IOException;
}
