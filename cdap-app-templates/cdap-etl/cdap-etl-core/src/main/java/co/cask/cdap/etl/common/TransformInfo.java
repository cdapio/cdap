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

import javax.annotation.Nullable;

/**
 * Class to encapsulate id of the transform and the error dataset used in that transform stage.
 */
public class TransformInfo {
  private final String transformId;
  private final String errorDatasetName;

  public TransformInfo(String transformId, @Nullable String errorDatasetName) {
    this.transformId = transformId;
    this.errorDatasetName = errorDatasetName;
  }

  public String getTransformId() {
    return transformId;
  }

  @Nullable
  public String getErrorDatasetName() {
    return errorDatasetName;
  }
}
