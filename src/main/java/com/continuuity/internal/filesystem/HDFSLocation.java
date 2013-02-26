/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.filesystem;

import com.continuuity.filesystem.Location;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * A concrete implementation of {@link Location} for the HDFS filesystem.
 */
public final class HDFSLocation implements Location {
  private final FileSystem fs;
  private final Path path;

  /**
   * Created by the {@link HDFSLocationFactory}
   *
   * @param fs   An instance of {@link FileSystem}
   * @param path of the file.
   */
  public HDFSLocation(FileSystem fs, String path) {
    this.fs = fs;
    this.path = new Path(path);
  }

  /**
   * Created by the {@link HDFSLocationFactory}
   *
   * @param fs  An instance of {@link FileSystem}
   * @param uri of the file.
   */
  public HDFSLocation(FileSystem fs, URI uri) {
    this.fs = fs;
    this.path = new Path(uri);
  }

  /**
   * Checks if the this location exists on HDFS.
   *
   * @return true if found; false otherwise.
   * @throws IOException
   */
  @Override
  public boolean exists() throws IOException {
    return fs.exists(path);
  }

  /**
   * @return An {@link InputStream} for this location on HDFS.
   * @throws IOException
   */
  @Override
  public InputStream getInputStream() throws IOException {
    return fs.open(path);
  }

  /**
   * @return An {@link OutputStream} for this location on HDFS.
   * @throws IOException
   */
  @Override
  public OutputStream getOutputStream() throws IOException {
    return fs.append(path);
  }

  /**
   * Appends the child to the current {@link Location} on HDFS.
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
    return new HDFSLocation(fs, child);
  }

  /**
   * @return Returns the name of the file or directory denoteed by this abstract pathname.
   */
  @Override
  public String getName() {
    return path.getName();
  }

  /**
   * @return A {@link URI} for this location on HDFS.
   */
  @Override
  public URI toURI() {
    return path.toUri();
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
    return fs.delete(path, false);
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
    fs.deleteOnExit(path);
  }

  /**
   * Creates the directory named by this abstract pathname, including any necessary
   * but nonexistent parent directories.
   *
   * @return true if and only if the renaming succeeded; false otherwise
   */
  @Override
  public boolean mkdirs() throws IOException {
    return fs.mkdirs(path);
  }

  /**
   * @return Length of file.
   */
  @Override
  public long length() throws IOException {
    return fs.getFileStatus(path).getLen();
  }
}
