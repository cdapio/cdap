/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.io.Schema;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * This class consolidates the functionalities related to find schemas
 * that are compatible for a connection.
 */
public final class SchemaFinder {
  /**
   * Given two schema's checks if there exists compatibility or equality.
   *
   * @param output Output {@link Schema} of source flowlet.
   * @param input  Input {@link Schema} of target flowlet.
   * @return true if and only if they are equal or compatible with constraints
   */
  public static boolean checkSchema(Set<Schema> output, Set<Schema> input) {
    for(Schema outputSchema : output) {
      int equal = 0;
      int compatible = 0;
      for(Schema inputSchema : input) {
        if(outputSchema.equals(inputSchema)) {
          equal++;
        }
        if(outputSchema.isCompatible(inputSchema)) {
          compatible++;
        }
      }

      // There is max of one output schema that is capable of handling
      // the input.
      if(equal > 1) {
        return false;
      }

      // There is min of 1 compatible in light of none being equal to handle
      // input.
      if(equal < 1 && compatible < 1) {
        return false;
      }
    }
    return true;
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
   * @param output
   * @param input
   * @return
   */
  @Nullable
  public static Schema findSchema(Set<Schema> output, Set<Schema> input) {
    Schema compatibleSchema = null;
    int compatible = 0;

    for(Schema outputSchema : output) {
      for(Schema inputSchema : input) {
        if(outputSchema.equals(inputSchema)) {
          return inputSchema;
        }

        if(outputSchema.isCompatible(inputSchema)) {
          compatibleSchema = inputSchema;
          compatible++;
        }
      }
    }

    // If there are more than one compatible, then it's a problem
    // we should have only strictly one.
    if(compatible == 1) {
      return compatibleSchema;
    }

    return null;
  }
}
