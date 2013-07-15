package com.continuuity.api.data.dataset;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * This is the {@link DataSet} implementation of named files. Other DataSets can be
 * defined by embedding instances of {@link FileDataSet} (and other DataSets).
 *
 * A {@link FileDataSet} provides reading and writing operations for a single file.
 *
 * The {@link FileDataSet} relies on injection of the data fabric by the execution context.
 * (@see DataSet).
 */
@Beta
public class FileDataSet extends DataSet {
  private static final String ATTR_FILE_PATH = "filePath";
  // This is the dataset that executes the actual operations. Using a delegate
  // allows us to inject a different implementation.
  private FileDataSet delegate = null;

  // path to a file
  private URI path;

  /**
   * Constructor by name.
   * @param name the name of the dataset
   * @param path the path to the file
   */
  public FileDataSet(String name, URI path) {
    super(name);
    this.path = path;
  }

  /**
   * Runtime initialization, only calls the super class.
   * @param spec the data set spec for this data set
   */
  public FileDataSet(DataSetSpecification spec) {
    super(spec);
    this.path = URI.create(spec.getProperty(ATTR_FILE_PATH));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .property(ATTR_FILE_PATH, this.path.toString())
      .create();
  }

  /**
   * Sets the {@link FileDataSet} to which all operations are delegated. This can be used
   * to inject different implementations.
   * @param dataSet the implementation to delegate to
   */
  public void setDelegate(FileDataSet dataSet) {
    this.delegate = dataSet;
  }

  /**
   * Checks if the this file exists.
   *
   * @return true if found; false otherwise.
   * @throws IOException
   */
  public boolean exists() throws IOException {
    Preconditions.checkState(this.delegate != null, "Not supposed to call runtime methods at configuration time.");
    return delegate.exists();
  }

  /**
   * Deletes the file.
   *
   * @return true if and only if the file is successfully deleted; false otherwise.
   */
  public boolean delete() throws IOException {
    Preconditions.checkState(this.delegate != null, "Not supposed to call runtime methods at configuration time.");
    return delegate.delete();
  }

  /**
   * @return An {@link java.io.InputStream} of this file.
   * @throws IOException
   */
  public InputStream getInputStream() throws IOException {
    Preconditions.checkState(this.delegate != null, "Not supposed to call runtime methods at configuration time.");
    return delegate.getInputStream();
  }

  /**
   * @return An {@link java.io.OutputStream} of this file.
   * @throws IOException
   */
  public OutputStream getOutputStream() throws IOException {
    Preconditions.checkState(this.delegate != null, "Not supposed to call runtime methods at configuration time.");
    return delegate.getOutputStream();
  }

  /**
   * @return path of the file
   */
  public URI getPath() {
    return path;
  }
}
