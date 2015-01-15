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
package co.cask.cdap.data.file;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Iterator;

/**
 * This interface represents classes that can write data to file.
 *
 * @param <T> Type of data that can be written to the file.
 */
public interface FileWriter<T> extends Closeable, Flushable {

  /**
   * Appends an event to the file.
   *
   * @param event event to append
   * @throws IOException if fail to append
   */
  void append(T event) throws IOException;

  /**
   * Appends multiple events to the file. This method will block until the iterator ends.
   *
   * @param events an {@link Iterator} that provides events to append
   * @throws IOException if fail to append
   */
  void appendAll(Iterator<? extends T> events) throws IOException;
}
