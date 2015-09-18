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

import co.cask.cdap.etl.api.Transformation;

/**
 * Class that encapsulates {@link co.cask.cdap.etl.api.Transform} and transformId
 * and {@link co.cask.cdap.etl.common.StageMetrics}
 */
public class TransformDetail {

  private final String transformId;
  private final Transformation transformation;
  private final StageMetrics metrics;

  public TransformDetail(String transformId, Transformation transform, StageMetrics metrics) {
    this.transformation = transform;
    this.transformId = transformId;
    this.metrics = metrics;
  }

  public TransformDetail(TransformDetail transformDetail, Transformation transformation) {
    this.transformation = transformation;
    this.transformId = transformDetail.getTransformId();
    this.metrics = transformDetail.getMetrics();
  }

  public Transformation getTransformation() {
    return transformation;
  }

  public String getTransformId() {
    return transformId;
  }

  public StageMetrics getMetrics() {
    return metrics;
  }
}
