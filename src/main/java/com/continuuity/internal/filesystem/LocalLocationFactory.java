/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.filesystem;

import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;

import java.net.URI;

/**
 *
 */
public final class LocalLocationFactory implements LocationFactory {

  @Override
  public Location create(String path) {
    return new LocalLocation(path);
  }

  @Override
  public Location create(URI uri) {
    return new LocalLocation(uri);
  }
}
