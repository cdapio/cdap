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

package co.cask.cdap.internal.app.program;

import co.cask.cdap.proto.ProgramType;

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
  public static String getMetricContextId(ProgramType programType) {
    switch (programType) {
      case FLOW:
        return "f";
      case PROCEDURE:
        return "p";
      case MAPREDUCE:
        return "b";
      case WORKFLOW:
        return "w";
      case SPARK:
        return "s";
      case SERVICE:
        return "u";
      case WORKER:
        return "w";
      default:
        return "unknown";
    }
  }
}
