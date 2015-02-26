/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.transaction.ForwardingTransactionAware;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConfigurer;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.queue.SimpleQueueSpecificationGenerator;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.hash.Hashing;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
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
                  .putString(program.getNamespaceId())
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
                                                           StreamAdmin streamAdmin, QueueAdmin queueAdmin,
                                                           TransactionExecutorFactory txExecutorFactory) {
    // Generate all queues specifications
    Id.Application appId = Id.Application.from(program.getNamespaceId(), program.getApplicationId());
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
      // Configure each stream consumer in the Flow. Also collects all queue configurers.
      final List<ConsumerGroupConfigurer> groupConfigurers = Lists.newArrayList();

      for (Map.Entry<QueueName, Map<Long, Integer>> row : queueConfigs.rowMap().entrySet()) {
        LOG.info("Queue config for {} : {}", row.getKey(), row.getValue());
        if (row.getKey().isStream()) {
          streamAdmin.configureGroups(row.getKey().toStreamId(), row.getValue());
        } else {
          groupConfigurers.add(new ConsumerGroupConfigurer(queueAdmin.getQueueConfigurer(row.getKey()),
                                                           row.getValue()));
        }
      }

      // Configure queue transactionally
      try {
        Transactions.createTransactionExecutor(txExecutorFactory, groupConfigurers)
          .execute(new TransactionExecutor.Subroutine() {
            @Override
            public void apply() throws Exception {
              for (ConsumerGroupConfigurer configurer : groupConfigurers) {
                configurer.configure();
              }
            }
          });
      } finally {
        for (ConsumerGroupConfigurer configurer : groupConfigurers) {
          Closeables.closeQuietly(configurer);
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
  public static void reconfigure(Iterable<QueueName> consumerQueues, final long groupId, final int instances,
                                 StreamAdmin streamAdmin, QueueAdmin queueAdmin,
                                 TransactionExecutorFactory txExecutorFactory) throws Exception {
    // Reconfigure stream and collects all queue configurers
    final List<QueueConfigurer> queueConfigurers = Lists.newArrayList();
    for (QueueName queueName : consumerQueues) {
      if (queueName.isStream()) {
        streamAdmin.configureInstances(queueName.toStreamId(), groupId, instances);
      } else {
        queueConfigurers.add(queueAdmin.getQueueConfigurer(queueName));
      }
    }

    // Reconfigure queue transactionally
    try {
      Transactions.createTransactionExecutor(txExecutorFactory, queueConfigurers)
        .execute(new TransactionExecutor.Subroutine() {
          @Override
          public void apply() throws Exception {
            for (QueueConfigurer queueConfigurer : queueConfigurers) {
              queueConfigurer.configureInstances(groupId, instances);
            }
          }
        });
    } finally {
      for (QueueConfigurer configurer : queueConfigurers) {
        Closeables.closeQuietly(configurer);
      }
    }
  }

  /**
   * Helper class for configuring queue with new consumer groups information.
   */
  private static final class ConsumerGroupConfigurer extends ForwardingTransactionAware implements Closeable {

    private final QueueConfigurer queueConfigurer;
    private final Map<Long, Integer> groupInfo;

    private ConsumerGroupConfigurer(QueueConfigurer queueConfigurer, Map<Long, Integer> groupInfo) {
      super(queueConfigurer);
      this.queueConfigurer = queueConfigurer;
      this.groupInfo = ImmutableMap.copyOf(groupInfo);
    }

    private void configure() throws Exception {
      queueConfigurer.configureGroups(groupInfo);
    }

    @Override
    public void close() throws IOException {
      queueConfigurer.close();
    }
  }


  private FlowUtils() {
  }
}
