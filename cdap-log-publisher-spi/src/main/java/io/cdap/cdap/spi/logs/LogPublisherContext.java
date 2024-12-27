/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.spi.logs;

import java.util.Map;

/**
 * Provides context information for {@link LogPublisher} initialization.
 */
public interface LogPublisherContext {

  /**
   * Properties are derived from the CDAP configuration. Configuration file path will be added as an
   * entry in the  properties.
   *
   * @return unmodifiable properties for the log publisher.
   */
  Map<String, String> getProperties();
}
