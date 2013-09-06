package com.continuuity.internal.app.runtime.flow;

import com.continuuity.app.program.Program;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.common.queue.QueueName;
import com.google.common.hash.Hashing;

/**
 * Set of static helper methods used by flow system.
 */
public final class FlowUtils {

  /**
   * Gets the number of consumer groups of a given queue based on the queue specifications.
   * @param queueSpecs Queue specifications to search for usage of given queue.
   * @param queueName Name of the queue.
   * @return Number of consumer groups of the given queue.
   */
  public static int getQueueConsumerGroups(Iterable<QueueSpecification> queueSpecs, QueueName queueName) {
    int numGroups = 0;
    for (QueueSpecification queueSpec : queueSpecs) {
      if (queueName.equals(queueSpec.getQueueName())) {
        numGroups++;
      }
    }
    return numGroups;
  }


  /**
   * Generates a queue consumer groupId for the given flowlet in the given program.
   */
  public static long generateConsumerGroupId(Program program, String flowletId) {
    return Hashing.md5().newHasher()
                  .putString(program.getAccountId())
                  .putString(program.getApplicationId())
                  .putString(program.getProgramName())
                  .putString(flowletId).hash().asLong();
  }


  private FlowUtils() {
  }
}
