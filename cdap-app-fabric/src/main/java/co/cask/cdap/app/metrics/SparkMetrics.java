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

package co.cask.cdap.app.metrics;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.common.metrics.MetricsCollector;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Metrics collector for {@link Spark} Programs
 */
public final class SparkMetrics extends AbstractProgramMetrics implements Externalizable {
  private static final long serialVersionUID = -5913108632034346101L;

  private static MetricsCollector metricsCollector;

  public SparkMetrics() {
    super(metricsCollector);
  }

  public SparkMetrics(MetricsCollector collector) {
    super(collector);
    metricsCollector = collector;
  }

  @Override
  public void writeExternal(ObjectOutput objectOutput) throws IOException {
    //No-op
  }

  @Override
  public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
    //No-op
  }
}
