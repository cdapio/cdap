/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.api.data.dataset;

import com.continuuity.api.annotation.Beta;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.data.DataSetSpecification;
import com.google.common.base.Supplier;

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

  // path to a file
  private URI path;

  private Supplier<FileDataSet> delegate = new Supplier<FileDataSet>() {
    @Override
    public FileDataSet get() {
      throw new IllegalStateException("Delegate is not set");
    }
  };

  /**
   * Constructor by name.
   * @param name the name of the dataset
   * @param path the path to the file
   */
  public FileDataSet(String name, URI path) {
    super(name);
    this.path = path;
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
      .property(ATTR_FILE_PATH, this.path.toString())
      .create();
  }

  @Override
  public void initialize(DataSetSpecification spec, DataSetContext context) {
    super.initialize(spec, context);
    this.path = URI.create(spec.getProperty(ATTR_FILE_PATH));
  }

  /**
   * Checks if the this file exists.
   *
   * @return true if found; false otherwise.
   * @throws IOException
   */
  public boolean exists() throws IOException {
    return delegate.get().exists();
  }

  /**
   * Deletes the file.
   *
   * @return true if and only if the file is successfully deleted; false otherwise.
   */
  public boolean delete() throws IOException {
    return delegate.get().delete();
  }

  /**
   * @return An {@link java.io.InputStream} of this file.
   * @throws IOException
   */
  public InputStream getInputStream() throws IOException {
    return delegate.get().getInputStream();
  }

  /**
   * @return An {@link java.io.OutputStream} of this file.
   * @throws IOException
   */
  public OutputStream getOutputStream() throws IOException {
    return delegate.get().getOutputStream();
  }

  /**
   * @return path of the file
   */
  public final URI getPath() {
    return path;
  }
}
