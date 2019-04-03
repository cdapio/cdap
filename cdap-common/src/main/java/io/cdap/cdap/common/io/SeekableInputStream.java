/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.common.io;

import org.apache.hadoop.fs.Seekable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Abstract base class for {@link InputStream} that implements the {@link Seekable} interface.
 */
public abstract class SeekableInputStream extends FilterInputStream implements Seekable {

  /**
   * Returns the current size of the input stream or -1 if the size is unknown.
   *
   * @return size of the input stream in bytes
   * @throws IOException if failed to determine the size
   */
  public abstract long size() throws IOException;

  protected SeekableInputStream(InputStream in) {
    super(in);
  }
}
