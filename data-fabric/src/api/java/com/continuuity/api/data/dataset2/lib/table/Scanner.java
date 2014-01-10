package com.continuuity.api.data.dataset2.lib.table;

import com.continuuity.common.utils.ImmutablePair;

import java.util.Map;

/**
 * Interface for table scan operation.
 */
public interface Scanner {

  public ImmutablePair<byte[], Map<byte[], byte[]>> next();

  public void close();

}
