/*
 * Copyright © 2017 Cask Data, Inc.
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

import java.io.IOException;

/**
 * Represent a destination that output can be sync to the underlying storage system.
 */
public interface Syncable {

  /**
   * Flushes data and sync to the underlying storage system.
   * Data should be persisted permanently after this call returns.
   *
   * @throws IOException if failed to perform the sync operation
   */
  void sync() throws IOException;
}
