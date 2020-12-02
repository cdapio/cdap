/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import java.io.IOException;

/**
 * CapabilityWriter interface
 */
public interface CapabilityWriter {

  /**
   * Adds or updates the capability status
   *
   * @param capability
   * @param status
   * @throws IOException
   */
  void addOrUpdateCapability(String capability, CapabilityStatus status, CapabilityConfig config) throws IOException;

  /**
   * Delete this capability
   *
   * @param capability
   * @throws IOException
   */
  void deleteCapability(String capability) throws IOException;
}
