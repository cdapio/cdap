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
 * {@link Location} is agnostic to the type of file system the resource is on.
 * </p>
 */
public interface Location {
  /**
   * Checks if the this location exists.
   *
   * @return true if found; false otherwise.
   * @throws IOException
   */
  boolean exists() throws IOException;

  /**
   * @return Returns the name of the file or directory denoteed by this abstract pathname.
   */
  String getName();

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
   * Returns a new instance of Location.
   * </p>
   *
   * @param child to be appended to this location.
   * @return A new instance of {@link Location}
   * @throws IOException
   */
  Location append(String child) throws IOException;

  /**
   * @return A {@link URI} for this location.
   */
  URI toURI();

  /**
   * Deletes the file or directory denoted by this abstract pathname. If this
   * pathname denotes a directory, then the directory must be empty in order
   * to be deleted.
   *
   * @return true if and only if the file or directory is successfully delete; false otherwise.
   */
  boolean delete() throws IOException;


  /**
   * Requests that the file or directory denoted by this abstract pathname be
   * deleted when the virtual machine terminates. Files (or directories) are deleted in
   * the reverse order that they are registered. Invoking this method to delete a file or
   * directory that is already registered for deletion has no effect. Deletion will be
   * attempted only for normal termination of the virtual machine, as defined by the
   * Java Language Specification.
   * <p>
   * Once deletion has been requested, it is not possible to cancel the request.
   * This method should therefore be used with care.
   * </p>
   */
  void deleteOnExit() throws IOException;

  /**
   * Creates the directory named by this abstract pathname, including any necessary
   * but nonexistent parent directories.
   *
   * @return true if and only if the renaming succeeded; false otherwise
   */
  boolean mkdirs() throws IOException;
}
