/*
 * Copyright © 2026 Cask Data, Inc.
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
package io.cdap.cdap.common.io;

import java.io.IOException;

/**
 * A replacement for the removed {@code com.google.common.io.OutputSupplier}.
 * Supplies an output stream of type {@code T}.
 *
 * @param <T> the type of output object supplied
 */
@FunctionalInterface
public interface OutputSupplier<T> {

  /**
   * Returns an output object that can be used for writing.
   *
   * @return the output object
   * @throws IOException if an I/O error occurs
   */
  T getOutput() throws IOException;
}
