/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import java.util.List;
import java.util.Map;

/**
 * <p>
 *   FieldLevelLineage is a data type for computing lineage for each field in a dataset.
 *   An instance of this type can be sent to be sent to platform through API for transform stages.
 * </p>
 */
public interface FieldLevelLineage {

  /**
   * <p>
   *   A BranchingTransformStepNode represents a linking between a {@link TransformStep} and a field of data.
   *   It contains data about how this {@link TransformStep} affected this field.
   * </p>
   */
  interface BranchingTransformStepNode {

    /**
     * @return the index of the {@link TransformStep} in {@link #getSteps()}
     */
    int getTransformStepNumber();

    /**
     * @return true if this {@link TransformStep} does not add this field
     */
    boolean continueBackward();

    /**
     * @return true if this {@link TransformStep} does not drop this field
     */
    boolean continueForward();

    /**
     * This map should contain every other field that was impacted by this field in this {@link TransformStep}.
     * @return A map from field name to the index of the next {@link TransformStep} using {@link #getLineage()}
     * Usage: getLineage().get(field).get(index)
     */
    Map<String, Integer> getImpactedBranches();

    /**
     * This map should contain every other field that impacted this field with this {@link TransformStep}.
     * @return A map from field name to the index of the previous {@link TransformStep} in {@link #getLineage()}
     * Usage: getLineage().get(field).get(index)
     */
    Map<String, Integer> getImpactingBranches();
  }

  /**
   * @return a list of all {@link TransformStep} executed by this Transform in order.
   */
  List<TransformStep> getSteps();

  /**
   * @return a mapping of field names to a list of {@link BranchingTransformStepNode} in order.
   */
  Map<String, List<BranchingTransformStepNode>> getLineage();
}
