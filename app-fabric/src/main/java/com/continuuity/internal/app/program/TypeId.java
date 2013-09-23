package com.continuuity.internal.app.program;

import com.continuuity.app.program.Type;

/**
 * Helper class for getting the program type id to use when emitting metrics.
 */
public final class TypeId {
  /**
   * Metric contexts are of the form {applicationId}.{programType}.{programId}.{optionalComponentId},
   * where programType is some string.
   *
   * @return id of the program type for use in metrics contexts.
   */
  public static String getMetricContextId(Type programType) {
    switch (programType) {
      case FLOW:
        return "f";
      case PROCEDURE:
        return "p";
      case MAPREDUCE:
        return "b";
      case WORKFLOW:
        return "w";
      default:
        return "unknown";
    }
  }
}
