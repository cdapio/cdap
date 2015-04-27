/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.cube;

import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Default implementation of {@link Aggregation}.
 */
public class DefaultAggregation implements Aggregation {
  private final List<String> aggregateDimensions;
  private final Set<String> requiredDimensions;

  /**
   * Creates instance of {@link DefaultAggregation} with all tags being optional.
   * @param aggregateDimensions tags to be included in aggregation.
   */
  public DefaultAggregation(List<String> aggregateDimensions) {
    this(aggregateDimensions, new HashSet<String>());
  }

  /**
   * Creates instance of {@link DefaultAggregation}.
   * <p/>
   * See also {@link Aggregation#getDimensionNames()} for more info on aggregateDimensions.
   *
   * @param aggregateDimensions dimensions to be included in aggregation.
   * @param requiredDimensions dimensions that must be present in {@link co.cask.cdap.api.dataset.lib.cube.CubeFact}
   *                     for aggregated value to be stored.
   */
  public DefaultAggregation(List<String> aggregateDimensions, Collection<String> requiredDimensions) {
    this.aggregateDimensions = ImmutableList.copyOf(aggregateDimensions);
    this.requiredDimensions = ImmutableSet.copyOf(requiredDimensions);
  }

  @Override
  public List<String> getDimensionNames() {
    return aggregateDimensions;
  }

  public Set<String> getRequiredDimensions() {
    return requiredDimensions;
  }

  @Override
  public boolean accept(CubeFact fact) {
    return fact.getDimensionValues().keySet().containsAll(requiredDimensions);
  }
}
