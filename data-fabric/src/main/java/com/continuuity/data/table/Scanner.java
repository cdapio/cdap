package com.continuuity.data.table;

import com.continuuity.common.utils.ImmutablePair;

import java.util.Map;

/**
 * Interface for table scan operation.
 */
public interface Scanner {

  public ImmutablePair<byte[], Map<byte[], byte[]>> next();

  public void close();

}
