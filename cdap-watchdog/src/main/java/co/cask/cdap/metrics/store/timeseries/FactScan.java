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

package co.cask.cdap.metrics.store.timeseries;

import java.util.List;

/**
 * Defines a scan over facts in a {@link FactTable}.
 * <p/>
 * NOTE: it will only scan those facts that at the time of writing had all given tags (some could have null values).
 */
public final class FactScan {
  private final List<TagValue> tagValues;
  private final String measureName;
  private final long startTs;
  private final long endTs;

  public FactScan(long startTs, long endTs, String measureName, List<TagValue> tagValues) {
    this.endTs = endTs;
    this.startTs = startTs;
    this.measureName = measureName;
    this.tagValues = tagValues;
  }

  public List<TagValue> getTagValues() {
    return tagValues;
  }

  public String getMeasureName() {
    return measureName;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }
}
