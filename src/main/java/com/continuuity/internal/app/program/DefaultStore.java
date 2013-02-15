/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.app.program.Store;
import com.continuuity.app.program.Version;
import com.google.inject.Inject;

import java.util.List;

/**
 *
 */
public class DefaultStore implements Store {

  /**
   * @return A list of available version of the program.
   */
  @Override
  public List<Version> getAvailableVersions() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * @return Current active version of this {@link com.continuuity.app.program.Program}
   */
  @Override
  public Version getCurrentVersion() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Deletes a <code>version</code> of this {@link com.continuuity.app.program.Program}
   *
   * @param version of the {@link com.continuuity.app.program.Program} to be deleted.
   */
  @Override
  public void delete(Version version) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Deletes all the versions of this {@link com.continuuity.app.program.Program}
   */
  @Override
  public void deleteAll() {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
