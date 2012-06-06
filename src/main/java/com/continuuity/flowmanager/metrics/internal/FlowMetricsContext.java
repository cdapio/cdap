package com.continuuity.flowmanager.metrics.internal;

import com.continuuity.flowmanager.metrics.CMetricsContext;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
final class FlowMetricsContext implements CMetricsContext {
  private final ImmutableMap<String, String> fields;

  public FlowMetricsContext(ImmutableMap<String, String> fields) {
    this.fields = fields;
  }

  @Override
  public String getScope() {
    return String.format("%s.%s.%s.%s", fields.get("accountid"), fields.get("app"),
      fields.get("version"), fields.get("rid"));

  }

  @Override
  public String getName(String metric) {
    return String.format("%s.%s.%s.%s", fields.get("flow"),
      fields.get("flowlet"), fields.get("instance"), metric);
  }

  @Override
  public String getField(String name) {
    return fields.get(name);
  }
}
