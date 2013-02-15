/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.filesystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * This interface defines the location and operations of a resource on the filesystem.
 * <p>
 *   {@link Location} is agnostic to the type of file system the resource is on.
 * </p>
 */
public interface Location {
  /**
   * Checks if the this location exists.
   * @return true if found; false otherwise.
   * @throws IOException
   */
  boolean exists() throws IOException;

  /**
   * @return Returns the name of the file or directory denoteed by this abstract pathname.
   */
  String getUri();

  /**
   * @return An {@link InputStream} for this location.
   * @throws IOException
   */
  InputStream getInputStream() throws IOException;

  /**
   * @return An {@link OutputStream} for this location.
   * @throws IOException
   */
  OutputStream getOutputStream() throws IOException;

  /**
   * Appends the child to the current {@link Location}.
   * <p>
   *   Returns a new instance of Location.
   * </p>
   * @param child to be appended to this location.
   * @return A new instance of {@link Location}
   * @throws IOException
   */
  Location append(String child) throws IOException;

  /**
   * @return A {@link URI} for this location.
   */
  URI toURI();
}
