/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.program;

import com.continuuity.archive.JarResources;
import com.continuuity.weave.filesystem.Location;

import java.io.IOException;

/**
 * Factory helper to create {@link Program}.
 */
public final class Programs {

  public static Program create(Location location) throws IOException {
    return new DefaultProgram(location, new JarResources(location));
  }

  private Programs() {
  }
}
