/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.filesystem;

import com.continuuity.filesystem.Location;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;

/**
 * A concrete implementation of {@link Location} for the Local filesystem.
 */
public final class LocalLocation implements Location {
  private final File file;

  /**
   * Created by the {@link LocalLocationFactory}
   *
   * @param uri of the file.
   */
  public LocalLocation(URI uri) {
    this(new File(uri));
  }

  /**
   * Created by the {@link LocalLocationFactory}
   *
   * @param path to the file.
   */
  public LocalLocation(String path) {
    this(new File(path));
  }

  /**
   * Used by the public constructors.
   *
   * @param file to the file.
   */
  private LocalLocation(File file) {
    this.file = file;
  }

  /**
   * Checks if the this location exists on local file system.
   *
   * @return true if found; false otherwise.
   * @throws IOException
   */
  @Override
  public boolean exists() throws IOException {
    return file.exists();
  }

  /**
   * @return An {@link InputStream} for this location on local filesystem.
   * @throws IOException
   */
  @Override
  public InputStream getInputStream() throws IOException {
    return new FileInputStream(file);
  }

  /**
   * @return An {@link OutputStream} for this location on local filesystem.
   * @throws IOException
   */
  @Override
  public OutputStream getOutputStream() throws IOException {
    return new FileOutputStream(file);
  }

  /**
   * @return Returns the name of the file or directory denoteed by this abstract pathname.
   */
  @Override
  public String getName() {
    return file.getName();
  }

  /**
   * Appends the child to the current {@link Location} on local filesystem.
   * <p>
   * Returns a new instance of Location.
   * </p>
   *
   * @param child to be appended to this location.
   * @return A new instance of {@link Location}
   * @throws IOException
   */
  @Override
  public Location append(String child) throws IOException {
    return new LocalLocation(new File(file, child));
  }

  @Override
  public Location getTempFile(String suffix) throws IOException {
    return new LocalLocation(file.getPath() + "." + UUID.randomUUID() + TEMP_FILE_SUFFIX);
  }

  /**
   * @return A {@link URI} for this location on local filesystem.
   */
  @Override
  public URI toURI() {
    return file.toURI();
  }

  /**
   * Deletes the file or directory denoted by this abstract pathname. If this
   * pathname denotes a directory, then the directory must be empty in order
   * to be deleted.
   *
   * @return true if and only if the file or directory is successfully delete; false otherwise.
   */
  @Override
  public boolean delete() throws IOException {
    return file.delete();
  }

  @Override
  public Location renameTo(Location destination) throws IOException {
    // destination will always be of the same type as this location
    boolean success = file.renameTo(((LocalLocation) destination).file);
    if (success) {
      return new LocalLocation(((LocalLocation) destination).file);
    } else {
      return null;
    }
  }

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
  @Override
  public void deleteOnExit() throws IOException {
    file.deleteOnExit();
  }

  /**
   * Creates the directory named by this abstract pathname, including any necessary
   * but nonexistent parent directories.
   *
   * @return true if and only if the renaming succeeded; false otherwise
   */
  @Override
  public boolean mkdirs() throws IOException {
    return file.mkdirs();
  }

  /**
   * @return Length of file.
   */
  @Override
  public long length() throws IOException {
    return file.length();
  }
}
