package com.continuuity.hive.server;

import com.google.common.util.concurrent.AbstractIdleService;


/**
 * Hive Server 2 service.
 */
public abstract class HiveServer extends AbstractIdleService {

  /**
   * Use a class loader to verify that HiveServer2 is present in the classpath.
   * @return true if HiveServer2 is present, false otherwise.
   */
  public static boolean isHivePresent() {
    try {
      Class.forName("org.apache.hive.service.server.HiveServer2");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
