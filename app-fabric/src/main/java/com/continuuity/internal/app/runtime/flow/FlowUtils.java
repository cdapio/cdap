package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.Id;
import com.continuuity.app.program.Program;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Set of static helper methods used by flow system.
 */
public final class FlowUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FlowUtils.class);

  /**
   * Generates a queue consumer groupId for the given flowlet in the given program.
   */
  public static long generateConsumerGroupId(Program program, String flowletId) {
    return generateConsumerGroupId(program.getId(), flowletId);
  }

  /**
   * Generates a queue consumer groupId for the given flowlet in the given program id.
   */
  public static long generateConsumerGroupId(Id.Program program, String flowletId) {
    return Hashing.md5().newHasher()
                  .putString(program.getAccountId())
                  .putString(program.getApplicationId())
                  .putString(program.getId())
                  .putString(flowletId).hash().asLong();
  }

  /**
   * Configures all queues being used in a flow.
   *
   * @return A Multimap from flowletId to QueueName where the flowlet is a consumer of.
   */
  public static Multimap<String, QueueName> configureQueue(Program program, FlowSpecification flowSpec,
                                                           StreamAdmin streamAdmin, QueueAdmin queueAdmin) {
    // Generate all queues specifications
    Id.Application appId = Id.Application.from(program.getAccountId(), program.getApplicationId());
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queueSpecs
      = new SimpleQueueSpecificationGenerator(appId).create(flowSpec);

    // For each queue in the flow, gather a map of consumer groupId to number of instances
    Table<QueueName, Long, Integer> queueConfigs = HashBasedTable.create();

    // For storing result from flowletId to queue.
    ImmutableSetMultimap.Builder<String, QueueName> resultBuilder = ImmutableSetMultimap.builder();

    // Loop through each flowlet
    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      String flowletId = entry.getKey();
      long groupId = FlowUtils.generateConsumerGroupId(program, flowletId);
      int instances = entry.getValue().getInstances();

      // For each queue that the flowlet is a consumer, store the number of instances for this flowlet
      for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.column(flowletId).values())) {
        queueConfigs.put(queueSpec.getQueueName(), groupId, instances);
        resultBuilder.put(flowletId, queueSpec.getQueueName());
      }
    }

    try {
      // For each queue in the flow, configure it through QueueAdmin
      for (Map.Entry<QueueName, Map<Long, Integer>> row : queueConfigs.rowMap().entrySet()) {
        LOG.info("Queue config for {} : {}", row.getKey(), row.getValue());
        if (row.getKey().isStream()) {
          streamAdmin.configureGroups(row.getKey(), row.getValue());
        } else {
          queueAdmin.configureGroups(row.getKey(), row.getValue());
        }
      }
      return resultBuilder.build();
    } catch (Exception e) {
      LOG.error("Failed to configure queues", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Reconfigures stream / queue consumer due to instances change.
   *
   * @param consumerQueues all queues that need to reconfigure
   * @param groupId consumer group id
   * @param instances consumer instance count
   */
  public static void reconfigure(Iterable<QueueName> consumerQueues, long groupId, int instances,
                                 StreamAdmin streamAdmin, QueueAdmin queueAdmin) throws Exception {
    // Then reconfigure stream/queue
    for (QueueName queueName : consumerQueues) {
      if (queueName.isStream()) {
        streamAdmin.configureInstances(queueName, groupId, instances);
      } else {
        queueAdmin.configureInstances(queueName, groupId, instances);
      }
    }
  }


  private FlowUtils() {
  }
}
