/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.filesystem;

import java.net.URI;

/**
 * {@link Location} is an abstraction provided over the LocalFilesystem location and HDFSFileSystem.
 * As the implementation of {@code Path} and {@code File} are incompatible, this approach allows us
 * to unify both approaches.
 */
public interface LocationFactory {
  /**
   * Creates an instance of {@link Location} based on string <code>path</code>
   *
   * @param path to the resource on the filesystem.
   * @return A instance of {@link Location}
   */
  Location create(String path);

  /**
   * Creates an instance of {@link Location} based on {@link URI} <code>uri</code>
   *
   * @param uri to the resource on the filesystem.
   * @return A instance of {@link Location}
   */
  Location create(URI uri);
}
