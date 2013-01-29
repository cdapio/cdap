package com.continuuity.manager.store;

import java.io.File;

/**
 *
 */
public interface Program {

  /**
   * @return a {@link File} which points to a valid program JAR.
   */
  File getJar();

  Attribute getAttribute();

  interface Attribute {

    String getAccount();

    String getUser();

    String getName();

    Version getVersion();

    long getCreateTime();

    interface Version {

      String getMajor();

      String getMinor();
    }
  }
}
