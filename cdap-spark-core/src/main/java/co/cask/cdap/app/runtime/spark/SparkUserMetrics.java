/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.metrics.Metrics;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A {@link Externalizable} implementation of {@link Metrics} used in Spark program execution.
 * It has no-op for serialize/deserialize operation, with all operations delegated to the {@link SparkRuntimeContext}
 * of the current execution context.
 */
public final class SparkUserMetrics implements Metrics, Externalizable {

  private final Metrics delegate;

  /**
   * Constructor. It delegates metrics operations to the current {@link SparkRuntimeContext}.
   */
  public SparkUserMetrics() {
    this(SparkRuntimeContextProvider.get());
  }

  /**
   * Creates an instance that delegates all metrics operations to the give {@link Metrics} delegate.
   */
  SparkUserMetrics(Metrics delegate) {
    this.delegate = delegate;
  }

  @Override
  public void count(String metricName, int delta) {
    delegate.count(metricName, delta);
  }

  @Override
  public void gauge(String metricName, long value) {
    delegate.gauge(metricName, value);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // no-op
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // no-op
  }
}
