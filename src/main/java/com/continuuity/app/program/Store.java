/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import java.util.List;

/**
 * {@link Store} operates on a {@link Program}. It's responsible
 * for managing the non-runtime lifecycle of a {@link Program}
 */
public interface Store {
  /**
   * Registers a {@link Program} with central meta data service.
   */
  public void register();

  /**
   * @return A list of available version of the program.
   */
  public List<Version> getAvailableVersions();

  /**
   * @return Current active version.
   */
  public Version getActiveVersion();

  /**
   * Deletes this {@link Program}
   */
  public void delete();
}
