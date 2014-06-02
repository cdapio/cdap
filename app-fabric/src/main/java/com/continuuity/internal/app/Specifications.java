package com.continuuity.internal.app;

/**
 * Temp util to convert app api V1 to internal api
 */
public final class Specifications {
  private Specifications() {}

  public static DefaultApplicationSpecification from(com.continuuity.api.ApplicationSpecification spec) {
    return new DefaultApplicationSpecification(spec.getName(), spec.getDescription(),
                                               spec.getStreams(), spec.getDataSets(),
                                               spec.getFlows(), spec.getProcedures(),
                                               spec.getMapReduce(), spec.getWorkflows());
  }
}
