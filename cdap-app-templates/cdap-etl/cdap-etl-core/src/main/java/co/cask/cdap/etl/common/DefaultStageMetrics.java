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
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.common.plugin.Caller;
import co.cask.cdap.etl.common.plugin.NoStageLoggingCaller;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

/**
 * Wrapper around the {@link Metrics} instance from CDAP that prefixes metric names with the ETL context the metric
 * was emitted from.
 */
public class DefaultStageMetrics implements StageMetrics, Externalizable {

  private Metrics metrics;
  private String prefix;
  private transient Caller caller;

  // Only used by Externalizable
  public DefaultStageMetrics() {
    this(null, "");
  }

  public DefaultStageMetrics(Metrics metrics, String stageName) {
    this.metrics = metrics;
    this.prefix = stageName + ".";
    this.caller = NoStageLoggingCaller.wrap(Caller.DEFAULT);
  }

  @Override
  public void count(final String metricName, final int delta) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        metrics.count(prefix + metricName, delta);
        return null;
      }
    });
  }

  @Override
  public void gauge(final String metricName, final long value) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        metrics.gauge(prefix + metricName, value);
        return null;
      }
    });
  }

  @Override
  public void pipelineCount(final String metricName, final int delta) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        metrics.count(metricName, delta);
        return null;
      }
    });
  }

  @Override
  public void pipelineGauge(final String metricName, final long value) {
    caller.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        metrics.gauge(metricName, value);
        return null;
      }
    });
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(metrics);
    out.writeObject(prefix);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    metrics = (Metrics) in.readObject();
    prefix = (String) in.readObject();
  }
}
