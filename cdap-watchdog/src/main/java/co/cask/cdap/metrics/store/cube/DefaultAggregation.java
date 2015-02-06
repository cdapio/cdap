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

package co.cask.cdap.metrics.store.cube;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Default implementation of {@link Aggregation}.
 */
public class DefaultAggregation implements Aggregation {
  private final List<String> aggregateTags;
  private final Set<String> requiredTags;

  /**
   * Creates instance of {@link DefaultAggregation}.
   * <p/>
   * See also {@link Aggregation#getTagNames()} for more info on aggregateTags.
   *
   * @param aggregateTags tags to be included in aggregation.
   * @param requiredTags tags that must be present in {@link CubeFact} for aggregated value to be stored.
   */
  public DefaultAggregation(List<String> aggregateTags, Collection<String> requiredTags) {
    this.aggregateTags = ImmutableList.copyOf(aggregateTags);
    this.requiredTags = ImmutableSet.copyOf(requiredTags);
  }

  @Override
  public List<String> getTagNames() {
    return aggregateTags;
  }

  @Override
  public boolean accept(CubeFact fact) {
    return fact.getTagValues().keySet().containsAll(requiredTags);
  }
}
