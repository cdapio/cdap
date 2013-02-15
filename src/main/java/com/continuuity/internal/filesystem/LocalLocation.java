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

/**
 * A concrete implementation of {@link Location} for the Local filesystem.
 */
final class LocalLocation implements Location {
  private final File file;

  /**
   * Created by the {@link LocalLocationFactory}
   * @param uri of the file.
   */
  public LocalLocation(URI uri) {
    this(new File(uri));
  }

  /**
   * Created by the {@link LocalLocationFactory}
   * @param path to the file.
   */
  public LocalLocation(String path) {
    this(new File(path));
  }

  /**
   * Used by the public constructors.
   * @param file to the file.
   */
  private LocalLocation(File file) {
    this.file = file;
  }

  /**
   * Checks if the this location exists on local file system.
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
  public String getUri() {
    return file.getName();
  }

  /**
   * Appends the child to the current {@link Location} on local filesystem.
   * <p>
   *   Returns a new instance of Location.
   * </p>
   * @param child to be appended to this location.
   * @return A new instance of {@link Location}
   * @throws IOException
   */
  @Override
  public Location append(String child) throws IOException {
    return new LocalLocation(new File(file, child));
  }

  /**
   * @return A {@link URI} for this location on local filesystem.
   */
  @Override
  public URI toURI() {
    return file.toURI();
  }
}
