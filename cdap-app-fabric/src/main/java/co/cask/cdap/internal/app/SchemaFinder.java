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

package co.cask.cdap.internal.app;

import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.api.data.schema.Schema;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class consolidates the functionalities related to find schemas
 * that are compatible for a connection.
 */
public final class SchemaFinder {
  /**
   * Given two schema's checks if there exists compatibility or equality.
   *
   * @param output Set of output {@link Schema}.
   * @param input  Set of input {@link Schema}.
   * @return true if and only if they are equal or compatible with constraints
   */
  public static boolean checkSchema(Set<Schema> output, Set<Schema> input) {
    return findSchema(output, input) != null;
  }

  /**
   * Finds the right schema to be used for the connections.
   * <p>
   *   A connection should have the following:
   *   <ul>
   *     <li>Equal overrides compatible : So if there is equal, we use that</li>
   *     <li>In case of compatible, we try to find one schema and only one. More than one is a error.</li>
   *   </ul>
   * </p>
   * @param output Set of output {@link Schema}.
   * @param input  Set of input {@link Schema}.
   * @return An {@link ImmutablePair} with first as output schema and second as input schema.
   */
  @Nullable
  public static ImmutablePair<Schema, Schema> findSchema(Set<Schema> output, Set<Schema> input) {
    ImmutablePair<Schema, Schema> compatibleSchema = null;

    for (Schema outputSchema : output) {
      for (Schema inputSchema : input) {
        if (outputSchema.equals(inputSchema)) {
          return new ImmutablePair<Schema, Schema>(inputSchema, outputSchema);
        }

        if (outputSchema.isCompatible(inputSchema)) {
          // If there are more than one compatible, then it's a problem
          // we should have only strictly one.
          if (compatibleSchema != null) {
            return null;
          }
          compatibleSchema = new ImmutablePair<Schema, Schema>(outputSchema, inputSchema);
        }
      }
    }

    return compatibleSchema;
  }

  private SchemaFinder() {}
}
