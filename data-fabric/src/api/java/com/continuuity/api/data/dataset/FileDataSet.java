package com.continuuity.api.data.dataset;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * TODO: fix docs
 * This is the DataSet implementation of named tables. Other DataSets can be
 * defined by embedding instances Table (and other DataSets).
 *
 * A Table can execute operations on its data, including read, write,
 * delete etc. Internally, the table delegates all operations to the
 * current transaction of the context in which this data set was
 * instantiated (a flowlet, or a procedure). Depending on that context,
 * operations may be executed immediately or deferred until the transaction
 * is committed.
 *
 * The Table relies on injection of the data fabric by the execution context.
 * (@see DataSet).
 */
@Beta
public class FileDataSet extends DataSet {
  private static final String ATTR_FILE_PATH = "filePath";
  // this is the Table that executed the actual operations. using a delegate
  // allows us to inject a different implementation.
  private FileDataSet delegate = null;

  private String path;

  /**
   * Constructor by name.
   * @param name the name of the table
   */
  public FileDataSet(String name, String path) {
    super(name);
    this.path = path;
  }

  /**
   * Runtime initialization, only calls the super class.
   * @param spec the data set spec for this data set
   */
  public FileDataSet(DataSetSpecification spec) {
    super(spec);
    this.path = spec.getProperty(ATTR_FILE_PATH);
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .property(ATTR_FILE_PATH, this.path)
      .create();
  }

  /**
   * Helper to return the name of the physical table. Currently the same as
   * the name of the (Table) data set.
   * @return the name of the underlying table in the data fabric
   */
  protected String tableName() {
    return this.getName();
  }

  /**
   * Sets the Table to which all operations are delegated. This can be used
   * to inject different implementations.
   * @param dataSet the implementation to delegate to
   */
  public void setDelegate(FileDataSet dataSet) {
    this.delegate = dataSet;
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
  public String getPath() {
    return path;
  }
}
