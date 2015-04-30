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

import co.cask.cdap.api.annotation.Batch;
import co.cask.cdap.api.annotation.HashPartition;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.RoundRobin;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.ConsumerGroupConfig;
import co.cask.cdap.data2.queue.DequeueStrategy;
import co.cask.cdap.data2.transaction.ForwardingTransactionAware;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.queue.QueueConfigurer;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.queue.SimpleQueueSpecificationGenerator;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.internal.lang.MethodVisitor;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.specification.FlowletMethod;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.hash.Hashing;
import com.google.common.io.Closeables;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

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
    // Use 'developer' in place of a program's namespace for programs in the 'default' namespace
    // to support backwards compatibility for queues and streams.
    String namespace = program.getNamespaceId();
    String backwardsCompatibleNamespace =
      Constants.DEFAULT_NAMESPACE.equals(namespace) ? Constants.DEVELOPER_ACCOUNT : namespace;
    return Hashing.md5().newHasher()
                  .putString(backwardsCompatibleNamespace)
                  .putString(program.getApplicationId())
                  .putString(program.getId())
                  .putString(flowletId).hash().asLong();
  }

  /**
   * Creates a {@link ConsumerGroupConfig} by inspecting the given process method.
   */
  public static ConsumerGroupConfig createConsumerGroupConfig(long groupId, int groupSize, Method processMethod) {
    // Determine input queue partition type
    HashPartition hashPartition = processMethod.getAnnotation(HashPartition.class);
    RoundRobin roundRobin = processMethod.getAnnotation(RoundRobin.class);
    DequeueStrategy strategy = DequeueStrategy.FIFO;
    String hashKey = null;

    Preconditions.checkArgument(!(hashPartition != null && roundRobin != null),
                                "Only one strategy allowed for process() method: %s", processMethod.getName());

    if (hashPartition != null) {
      strategy = DequeueStrategy.HASH;
      hashKey = hashPartition.value();
      Preconditions.checkArgument(!hashKey.isEmpty(), "Partition key cannot be empty: %s", processMethod.getName());
    } else if (roundRobin != null) {
      strategy = DequeueStrategy.ROUND_ROBIN;
    }

    return new ConsumerGroupConfig(groupId, groupSize, strategy, hashKey);
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

    // For each queue in the flow, gather all consumer groups information
    Multimap<QueueName, ConsumerGroupConfig> queueConfigs = HashMultimap.create();

    // Loop through each flowlet and generate the map from consumer flowlet id to queue
    ImmutableSetMultimap.Builder<String, QueueName> resultBuilder = ImmutableSetMultimap.builder();
    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      String flowletId = entry.getKey();

      for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.column(flowletId).values())) {
        resultBuilder.put(flowletId, queueSpec.getQueueName());
      }
    }

    // For each queue, gather all consumer groups.
    for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.values())) {
      QueueName queueName = queueSpec.getQueueName();
      queueConfigs.putAll(queueName, getAllConsumerGroups(program, flowSpec, queueName, queueSpecs));
    }

    try {
      // Configure each stream consumer in the Flow. Also collects all queue configurers.
      final List<ConsumerGroupConfigurer> groupConfigurers = Lists.newArrayList();

      for (Map.Entry<QueueName, Collection<ConsumerGroupConfig>> entry : queueConfigs.asMap().entrySet()) {
        LOG.info("Queue config for {} : {}", entry.getKey(), entry.getValue());
        if (entry.getKey().isStream()) {
          Map<Long, Integer> configs = Maps.newHashMap();
          for (ConsumerGroupConfig config : entry.getValue()) {
            configs.put(config.getGroupId(), config.getGroupSize());
          }
          streamAdmin.configureGroups(entry.getKey().toStreamId(), configs);
        } else {
          groupConfigurers.add(new ConsumerGroupConfigurer(queueAdmin.getQueueConfigurer(entry.getKey()),
                                                           entry.getValue()));
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
   * Gets all consumer group configurations for the given queue.
   */
  private static Set<ConsumerGroupConfig> getAllConsumerGroups(
    Program program, FlowSpecification flowSpec, QueueName queueName,
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queueSpecs) {

    Set<ConsumerGroupConfig> groupConfigs = Sets.newHashSet();
    SchemaGenerator schemaGenerator = new ReflectionSchemaGenerator();

    // Get all the consumers of this queue.
    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      String flowletId = entry.getKey();
      for (QueueSpecification queueSpec : Iterables.concat(queueSpecs.column(flowletId).values())) {
        if (!queueSpec.getQueueName().equals(queueName)) {
          continue;
        }

        try {
          // Inspect the flowlet consumer
          FlowletDefinition flowletDefinition = entry.getValue();
          Class<?> flowletClass = program.getClassLoader().loadClass(flowletDefinition.getFlowletSpec().getClassName());
          long groupId = generateConsumerGroupId(program, flowletId);

          addConsumerGroup(queueSpec, TypeToken.of(flowletClass), groupId,
                           flowletDefinition.getInstances(), schemaGenerator, groupConfigs);
        } catch (ClassNotFoundException e) {
          // There is no way for not able to load a Flowlet class as it should be verified during deployment.
          throw Throwables.propagate(e);
        }
      }
    }

    return groupConfigs;
  }

  /**
   * Finds all consumer group for the given queue from the given flowlet.
   */
  private static void addConsumerGroup(final QueueSpecification queueSpec,
                                       final TypeToken<?> flowletType,
                                       final long groupId, final int groupSize,
                                       final SchemaGenerator schemaGenerator,
                                       final Collection<ConsumerGroupConfig> groupConfigs) {

    final Set<FlowletMethod> seenMethods = Sets.newHashSet();
    Reflections.visit(null, flowletType, new MethodVisitor() {
      @Override
      public void visit(Object instance, TypeToken<?> inspectType,
                        TypeToken<?> declareType, Method method) throws Exception {
        if (!seenMethods.add(new FlowletMethod(method, inspectType))) {
          // The method is already seen. It can only happen if a children class override a parent class method and
          // is visiting the parent method, since the method visiting order is always from the leaf class walking
          // up the class hierarchy.
          return;
        }

        ProcessInput processInputAnnotation = method.getAnnotation(ProcessInput.class);
        if (processInputAnnotation == null) {
          // Consumer has to be process method
          return;
        }

        Set<String> inputNames = Sets.newHashSet(processInputAnnotation.value());
        if (inputNames.isEmpty()) {
          // If there is no input name, it would be ANY_INPUT
          inputNames.add(FlowletDefinition.ANY_INPUT);
        }

        TypeToken<?> dataType = inspectType.resolveType(method.getGenericParameterTypes()[0]);
        // For batch mode and if the parameter is Iterator, need to get the actual data type from the Iterator.
        if (method.isAnnotationPresent(Batch.class) && Iterator.class.equals(dataType.getRawType())) {
          Preconditions.checkArgument(dataType.getType() instanceof ParameterizedType,
                                      "Only ParameterizedType is supported for batch Iterator.");
          dataType = inspectType.resolveType(((ParameterizedType) dataType.getType()).getActualTypeArguments()[0]);
        }

        Schema schema = schemaGenerator.generate(dataType.getType());
        if (queueSpec.getInputSchema().equals(schema)
          && (inputNames.contains(queueSpec.getQueueName().getSimpleName())
          || inputNames.contains(FlowletDefinition.ANY_INPUT))) {
          groupConfigs.add(createConsumerGroupConfig(groupId, groupSize, method));
        }
      }
    });
  }

  /**
   * Delete the "system.queue.pending" metrics for a flow or for all flows in an app or a namespace.
   *
   * @param namespace the namespace id; may only be null if the appId and flowId are null
   * @param appId the application id; may only be null if the flowId is null
   */
  public static void deleteFlowPendingMetrics(MetricStore metricStore,
                                              @Nullable String namespace,
                                              @Nullable String appId,
                                              @Nullable String flowId)
    throws Exception {
    Preconditions.checkArgument(namespace != null || appId == null, "Namespace may only be null if AppId is null");
    Preconditions.checkArgument(appId != null || flowId == null, "AppId may only be null if FlowId is null");
    Collection<String> names = Collections.singleton("system.queue.pending");
    Map<String, String> tags = Maps.newHashMap();
    if (namespace != null) {
      tags.put(Constants.Metrics.Tag.NAMESPACE, namespace);
      if (appId != null) {
        tags.put(Constants.Metrics.Tag.APP, appId);
        if (flowId != null) {
          tags.put(Constants.Metrics.Tag.FLOW, flowId);
        }
      }
    }
    LOG.info("Deleting 'system.queue.pending' metric for context {}", tags);
    // we must delete up to the current time - let's round up to the next second.
    long nextSecond = System.currentTimeMillis() / 1000 + 1;
    metricStore.delete(new MetricDeleteQuery(0L, nextSecond, names, tags));
  }

  /**
   * Helper class for configuring queue with new consumer groups information.
   */
  private static final class ConsumerGroupConfigurer extends ForwardingTransactionAware implements Closeable {

    private final QueueConfigurer queueConfigurer;
    private final List<ConsumerGroupConfig> groupConfigs;

    private ConsumerGroupConfigurer(QueueConfigurer queueConfigurer,
                                    Iterable<? extends ConsumerGroupConfig> groupConfigs) {
      this.queueConfigurer = queueConfigurer;
      this.groupConfigs = ImmutableList.copyOf(groupConfigs);
    }


    private void configure() throws Exception {
      queueConfigurer.configureGroups(groupConfigs);
    }

    @Override
    public void close() throws IOException {
      queueConfigurer.close();
    }

    @Override
    protected TransactionAware delegate() {
      return queueConfigurer;
    }
  }

  private FlowUtils() {
  }
}
