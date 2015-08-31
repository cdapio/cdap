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

package co.cask.cdap.dq;

import co.cask.cdap.dq.functions.CombinableAggregationFunction;

import java.util.Objects;

/**
 * Class for the object that will represent a given aggregation type. For now, this class only has a field
 * for the aggregationTypeName
 */
public class AggregationTypeValue {
  private final String name;
  private boolean isCombinable;

  public AggregationTypeValue(String name, boolean isCombinable) {
    this.name = name;
    this.isCombinable = isCombinable;
  }

  public String getAggregationTypeName() {
    return name;
  }

  /**
   * @return true if the aggregation is a {@link CombinableAggregationFunction}
   */
  public boolean isCombinable() {
    return isCombinable;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AggregationTypeValue)) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    AggregationTypeValue agTypeListObj = (AggregationTypeValue) obj;
    return Objects.equals(name, agTypeListObj.name)
      && Objects.equals(isCombinable, agTypeListObj.isCombinable);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, isCombinable);
  }
}
