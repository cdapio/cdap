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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.metrics.Metrics;

/**
 * Wrapper around the {@link Metrics} instance from CDAP that prefixes metric names with the ETL context the metric
 * was emitted from.
 */
public class StageMetrics implements Metrics {
  private final Metrics metrics;
  private final String prefix;

  /**
   * Types of ETL stages.
   */
  public enum Type {
    SOURCE,
    SINK,
    TRANSFORM;

    @Override
    public String toString() {
      return name().toLowerCase();
    }
  }

  public StageMetrics(Metrics metrics, Type stageType, String name) {
    this.metrics = metrics;
    this.prefix = stageType.toString() + "." + name + ".";
  }

  @Override
  public void count(String s, int i) {
    metrics.count(prefix + s, i);
  }

  @Override
  public void gauge(String s, long l) {
    metrics.gauge(prefix + s, l);
  }
}
