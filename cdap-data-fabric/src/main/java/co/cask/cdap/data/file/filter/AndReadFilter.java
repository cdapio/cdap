/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data.file.filter;

import co.cask.cdap.data.file.ReadFilter;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AND multiple @{link ReadFilter}s.
 */
public final class AndReadFilter extends ReadFilter {
  private final List<ReadFilter> filters;

  // The one that reject timestamp
  private ReadFilter timestampRejectFilter;

  public AndReadFilter(ReadFilter...filters) {
    this.filters = ImmutableList.copyOf(filters);
  }

  @Override
  public void reset() {
    timestampRejectFilter = null;
  }

  @Override
  public boolean acceptOffset(long offset) {
    for (ReadFilter filter : filters) {
      if (!filter.acceptOffset(offset)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean acceptTimestamp(long timestamp) {
    for (ReadFilter filter : filters) {
      if (!filter.acceptTimestamp(timestamp)) {
        timestampRejectFilter = filter;
        return false;
      }
    }
    return true;
  }

  @Override
  public long getNextTimestampHint() {
    // If there is filter rejecting timestamp, use it to return hint
    return (timestampRejectFilter == null) ? -1L : timestampRejectFilter.getNextTimestampHint();
  }
}
