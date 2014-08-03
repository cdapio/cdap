/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.metrics.query;

import com.google.common.base.Objects;

/**
 * class to identify a unique timeseries, which is a 4 tuple of context, metric, tag, and runid.
 */
public final class TimeseriesId {
  private final String context;
  private final String metric;
  private final String tag;
  private final String runId;

  public TimeseriesId(String context, String metric, String tag, String runId) {
    this.context = context;
    this.metric = metric;
    this.tag = tag;
    this.runId = runId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TimeseriesId) || o == null) {
      return false;
    }
    TimeseriesId other = (TimeseriesId) o;
    return Objects.equal(context, other.context) &&
      Objects.equal(metric, other.metric) &&
      Objects.equal(tag, other.tag) &&
      Objects.equal(runId, other.runId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(context, metric, tag, runId);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("context", context)
      .add("metric", metric)
      .add("tag", tag)
      .add("runId", runId)
      .toString();
  }
}
