/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.file;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Iterator;

/**
 * Provides utility methods for interacting with {@link FileWriter}.
 */
public final class FileWriters {

  /**
   * Creates a {@link FileWriter} that writes to the given {@link FileWriter} with each event transformed by the
   * given transformation function.
   *
   * @param writer the {@link FileWriter} to write to
   * @param transform the transformation function for each individual event
   * @param <U> source type
   * @param <V> target type
   * @return a new instance of {@link FileWriter}
   */
  public static <U, V> FileWriter<U> transform(final FileWriter<V> writer, final Function<U, V> transform) {
    return new FileWriter<U>() {
      @Override
      public void append(U event) throws IOException {
        writer.append(transform.apply(event));
      }

      @Override
      public void appendAll(Iterator<? extends U> events) throws IOException {
        writer.appendAll(Iterators.transform(events, transform));
      }

      @Override
      public void close() throws IOException {
        writer.close();
      }

      @Override
      public void flush() throws IOException {
        writer.flush();
      }
    };
  }

  private FileWriters() {
  }
}
