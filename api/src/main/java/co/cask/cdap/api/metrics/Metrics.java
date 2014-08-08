/*
 * Copyright 2014 Cask, Inc.
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

/**
 * Defines a way to collect user-defined metrics.
 * To use it, just add a Metrics field in a CDAP application element, for example a Flowlet, and start using it.
 */
public interface Metrics {
  /**
   * Increases the value of the specific counter by delta.
   * @param counterName Name of the counter. Use alphanumeric characters in metric names.
   * @param delta The value to increase by.
   */
  void count(String counterName, int delta);
}

