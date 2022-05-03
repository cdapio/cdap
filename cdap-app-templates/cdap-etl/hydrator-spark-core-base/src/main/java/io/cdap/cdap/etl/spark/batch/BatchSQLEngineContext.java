/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.SQLEngineContext;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.preview.DataTracer;
import org.apache.twill.api.RunId;

import java.util.Map;

/**
 * Context implementation for an SQL engine.
 */
public class BatchSQLEngineContext implements SQLEngineContext {
  private final RuntimeContext runtimeContext;
  private final Metrics metrics;

  public BatchSQLEngineContext(RuntimeContext runtimeContext, Metrics metrics) {
    this.runtimeContext = runtimeContext;
    this.metrics = metrics;
  }

  @Override
  public ApplicationSpecification getApplicationSpecification() {
    return runtimeContext.getApplicationSpecification();
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeContext.getRuntimeArguments();
  }

  @Override
  public String getClusterName() {
    return runtimeContext.getClusterName();
  }

  @Override
  public String getNamespace() {
    return runtimeContext.getNamespace();
  }

  @Override
  public RunId getRunId() {
    return runtimeContext.getRunId();
  }

  @Override
  public Admin getAdmin() {
    return runtimeContext.getAdmin();
  }

  @Override
  public DataTracer getDataTracer(String dataTracerName) {
    return runtimeContext.getDataTracer(dataTracerName);
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public boolean isFeatureEnabled(String name) {
    return runtimeContext.isFeatureEnabled(name);
  }
}
