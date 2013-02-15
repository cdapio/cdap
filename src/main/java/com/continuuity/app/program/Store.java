/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.filesystem.Location;

import java.util.List;

/**
 * {@link Store} operates on a {@link Program}. It's responsible
 * for managing the non-runtime lifecycle of a {@link Program}
 */
public interface Store {
  /**
   * @return A list of available version of the program.
   */
  public List<Version> getAvailableVersions();

  /**
   * @return Current active version of this {@link Program}
   */
  public Version getCurrentVersion();

  /**
   * Deletes a <code>version</code> of this {@link Program}
   *
   * @param version of the {@link Program} to be deleted.
   */
  public void delete(Version version);

  /**
   * Deletes all the versions of this {@link Program}
   */
  public void deleteAll();
}
