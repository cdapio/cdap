package com.continuuity.metrics.query;

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
    return context.equals(other.context) && metric.equals(other.metric)
      && (tag == null) ? (other.tag == null) : tag.equals(other.tag)
      && (runId == null) ? (other.runId == null) : runId.equals(other.runId);
  }

  @Override
  public int hashCode() {
    int hash = context.hashCode();
    hash = 31 * hash + metric.hashCode();
    hash = 31 * hash + ((tag == null) ? 0 : tag.hashCode());
    hash = 31 * hash + ((runId == null) ? 0 : runId.hashCode());
    return hash;
  }
}