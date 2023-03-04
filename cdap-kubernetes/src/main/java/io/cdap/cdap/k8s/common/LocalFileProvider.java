/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.k8s.common;

import java.io.File;
import java.io.IOException;

/**
 * An interface providing a way of writing to local files. Provides a pluggable interface for
 * unit-testing file-based logic.
 */
public interface LocalFileProvider {

  /**
   * Returns a writable file object reference based on the provided name. Creates any necessary
   * directories for the object prior to returning the reference.
   *
   * @param path The path of the file
   * @return a File object
   */
  File getWritableFileRef(String path) throws IOException;
}
