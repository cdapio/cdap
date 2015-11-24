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
import co.cask.cdap.etl.api.Transformation;

import java.util.Map;

/**
 * Class that encapsulates {@link co.cask.cdap.etl.api.Transform} and transformId
 * and {@link Metrics}
 */
public class TransformDetail {

  private final Map<String, Transformation> transformationMap;
  private final Metrics metrics;

  public TransformDetail(Map<String, Transformation> transformationMap, Metrics metrics) {
    this.transformationMap = transformationMap;
    this.metrics = metrics;
  }

  public TransformDetail(TransformDetail existing, Map<String, Transformation> transformationMap) {
    this.transformationMap = transformationMap;
    this.metrics = existing.getMetrics();
  }

  public Transformation getTransformation(String stageName) {
    return transformationMap.get(stageName);
  }


  public Map<String, Transformation> getTransformationMap() {
    return transformationMap;
  }

  public Metrics getMetrics() {
    return metrics;
  }
}
