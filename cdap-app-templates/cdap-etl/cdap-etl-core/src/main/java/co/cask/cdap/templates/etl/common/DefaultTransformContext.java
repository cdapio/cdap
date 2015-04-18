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

package co.cask.cdap.templates.etl.common;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.templates.etl.api.StageContext;
import co.cask.cdap.templates.etl.api.StageSpecification;

import java.util.Map;

/**
 * Context for the Transform Stage.
 */
public class DefaultTransformContext implements StageContext {
  private final StageSpecification specification;
  private final Map<String, String> runtimeArgs;
  private final Metrics metrics;

  public DefaultTransformContext(StageSpecification specification, Map<String, String> runtimeArgs, Metrics metrics) {
    this.specification = specification;
    this.runtimeArgs = runtimeArgs;
    this.metrics = new StageMetrics(metrics, StageMetrics.Type.TRANSFORM, specification.getName());
  }

  @Override
  public StageSpecification getSpecification() {
    return specification;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return runtimeArgs;
  }
}
