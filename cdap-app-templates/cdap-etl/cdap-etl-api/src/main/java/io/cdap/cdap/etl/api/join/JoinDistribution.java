/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.join;

import io.cdap.cdap.etl.api.join.error.BroadcastError;
import io.cdap.cdap.etl.api.join.error.DistributionSizeError;
import io.cdap.cdap.etl.api.join.error.DistributionStageError;
import io.cdap.cdap.etl.api.join.error.JoinError;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Join distribution settings for salting/exploding datasets to resolve skew
 */
public class JoinDistribution {

  private final int distributionFactor;
  private final String skewedStageName;

  public JoinDistribution(Integer distributionFactor, String skewedStageName) {
    this.distributionFactor = distributionFactor;
    this.skewedStageName = skewedStageName;
  }

  public int getDistributionFactor() {
    return distributionFactor;
  }

  public String getSkewedStageName() {
    return skewedStageName;
  }

  public Collection<JoinError> validate(List<JoinStage> stages) {
    List<JoinError> errors = new ArrayList<>();

    if (stages.size() > 2) {
      errors.add(new JoinError("Only two stages can be joined if a distribution factor is specified"));
    }

    if (skewedStageName == null) {
      errors.add(new DistributionStageError("Distribution requires skewed stage name to be defined"));
    }

    if (distributionFactor < 1) {
      errors.add(new DistributionSizeError("Distribution size must be greater than 0"));
    }

    //If skewedStageName does not match any of the names in stages
    JoinStage leftStage = stages.stream().filter(s -> s.getStageName().equals(skewedStageName)).findFirst()
      .orElse(null);

    if (leftStage == null) {
      errors.add(
        new DistributionStageError(String.format("Skewed stage '%s' does not match any of the specified " +
          "stages", skewedStageName)));
    } else if (!leftStage.isRequired()) {
      errors.add(
        new DistributionStageError(String.format("Distribution only supports inner or left outer joins, the skewed " +
          "stage '%s' must be required", skewedStageName)));
    }

    if (stages.stream().anyMatch(JoinStage::isBroadcast)) {
      errors.add(new BroadcastError("Distribution cannot be used if either stage will be broadcast"));
    }

    return errors;
  }
}
