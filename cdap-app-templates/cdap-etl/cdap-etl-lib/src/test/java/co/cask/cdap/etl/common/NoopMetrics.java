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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.metrics.Metrics;

/**
 * No op metrics implementation for tests.
 */
public class NoopMetrics implements Metrics {
  public static final Metrics INSTANCE = new NoopMetrics();

  @Override
  public void count(String s, int i) {
    // no-op
  }

  @Override
  public void gauge(String s, long l) {
    // no-op
  }
}
