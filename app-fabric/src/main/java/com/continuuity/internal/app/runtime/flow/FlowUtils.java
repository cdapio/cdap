package com.continuuity.internal.app.runtime.flow;

import com.continuuity.app.program.Program;
import com.google.common.hash.Hashing;

/**
 * Set of static helper methods used by flow system.
 */
public final class FlowUtils {

  /**
   * Generates a queue consumer groupId for the given flowlet in the given program.
   */
  public static long generateConsumerGroupId(Program program, String flowletId) {
    return Hashing.md5().newHasher()
                  .putString(program.getAccountId())
                  .putString(program.getApplicationId())
                  .putString(program.getName())
                  .putString(flowletId).hash().asLong();
  }


  private FlowUtils() {
  }
}
