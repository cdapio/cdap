package com.continuuity.data.table;

import java.util.Map;

import com.continuuity.common.utils.ImmutablePair;

public interface Scanner {

  public ImmutablePair<byte[], Map<byte[], byte[]>> next();

  public void close();

}
