package com.continuuity.hive;

import com.google.common.util.concurrent.Service;

/**
 *
 */
public interface HiveServer extends Service {

  public int getHiveMetaStorePort();

  public int getHiveServerPort();

}
