/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.api.metrics;

import java.util.Map;

/**
 * A context of metrics collection.
 */
public interface MetricsContext extends MetricsCollector {
  /**
   * Creates child {@link MetricsContext} that inherits the metrics context from this one and adds extra context
   * information.
   * @param tags tags to add to the child metrics context
   * @return child {@link MetricsContext}
   */
  MetricsContext childContext(Map<String, String> tags);

  /**
   * Convenience method that acts as {@link #childContext(java.util.Map)} by supplying single tag.
   */
  MetricsContext childContext(String tagName, String tagValue);

  /**
   * @return tags that identify the context.
   */
  Map<String, String> getTags();
}
