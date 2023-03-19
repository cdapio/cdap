/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.lineage;

import javax.annotation.Nullable;

/**
 * Represents the Origin for a field.
 */
public final class Origin {
  private final String stageOperation;
  private final String marker;

  @Nullable
  public String getMarker() {
    return marker;
  }

  /**
   * Creates a new instance.
   *
   * @param stageOperation stage name followed by "." followed by operation name
   * @param marker only useful in case of multi-sources/multi-sinks
   */
  public Origin(String stageOperation, @Nullable String marker) {
    this.stageOperation = stageOperation;
    this.marker = marker;
  }

  public String getStageOperation() {
    return stageOperation;
  }
}
