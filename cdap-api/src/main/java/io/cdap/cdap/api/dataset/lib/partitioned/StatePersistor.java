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

package co.cask.cdap.api.dataset.lib.partitioned;

import javax.annotation.Nullable;

/**
 * Defines how some state will be persisted/managed.
 */
public interface StatePersistor {

  /**
   * @return the serialized bytes of the state of the partition consuming process; return null to indicate a fresh
   *         state of consuming (defaults to starting from the beginning).
   */
  @Nullable
  byte[] readState();

  /**
   * Writes the serialized bytes of the state of the partition consuming process. The bytes written in this method
   * should be available in the following call to readBytes(DatasetContext).
   * @param state the bytes to persist
   */
  void persistState(byte[] state);
}
