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

package co.cask.cdap.data2.dataset2.lib.timeseries;

import co.cask.cdap.api.dataset.lib.cube.DimensionValue;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

/**
 * Defines a scan over facts in a {@link FactTable}.
 * <p/>
 * NOTE: it will only scan those facts that at the time of writing had all given dimensions (some could have
 *       null values).
 */
public final class FactScan {
  private final List<DimensionValue> dimensionValues;
  private final Collection<String> measureNames;
  private final long startTs;
  private final long endTs;

  public FactScan(long startTs, long endTs, Collection<String> measureNames, List<DimensionValue> dimensionValues) {
    this.endTs = endTs;
    this.startTs = startTs;
    this.measureNames = measureNames;
    this.dimensionValues = ImmutableList.copyOf(dimensionValues);
  }

  public FactScan(long startTs, long endTs, String measureName, List<DimensionValue> dimensionValues) {
    this(startTs, endTs,
         measureName == null ? ImmutableList.<String>of() : ImmutableList.of(measureName), dimensionValues);
  }

  public FactScan(long startTs, long endTs, List<DimensionValue> dimensionValues) {
    this(startTs, endTs, ImmutableList.<String>of(), dimensionValues);
  }

  public List<DimensionValue> getDimensionValues() {
    return dimensionValues;
  }

  public Collection<String> getMeasureNames() {
    return measureNames;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }
}
