package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.metrics.FlowletMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.internal.app.runtime.AbstractContext;
import com.continuuity.internal.app.runtime.ProgramServiceDiscovery;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.ServiceDiscovered;

import java.io.Closeable;
import java.util.Map;

/**
 * Internal implementation of {@link FlowletContext}.
 */
final class BasicFlowletContext extends AbstractContext implements FlowletContext {

  private final String accountId;
  private final String flowId;
  private final String flowletId;
  private final long groupId;
  private final int instanceId;
  private final FlowletSpecification flowletSpec;

  private volatile int instanceCount;
  private final FlowletMetrics flowletMetrics;
  private final Arguments runtimeArguments;
  private final ProgramServiceDiscovery serviceDiscovery;

  private final MetricsCollector systemMetricsCollector;

  BasicFlowletContext(Program program, String flowletId, int instanceId, RunId runId, int instanceCount,
                      Map<String, Closeable> datasets, Arguments runtimeArguments,
                      FlowletSpecification flowletSpec, MetricsCollectionService metricsCollectionService,
                      ProgramServiceDiscovery serviceDiscovery) {
    super(program, runId, datasets);
    this.accountId = program.getAccountId();
    this.flowId = program.getName();
    this.flowletId = flowletId;
    this.groupId = FlowUtils.generateConsumerGroupId(program, flowletId);
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.runtimeArguments = runtimeArguments;
    this.flowletSpec = flowletSpec;
    this.flowletMetrics = new FlowletMetrics(metricsCollectionService, getApplicationId(), flowId, flowletId);
    this.systemMetricsCollector = getMetricsCollector(MetricsScope.REACTOR,
                                                      metricsCollectionService, getMetricContext());
    this.serviceDiscovery = serviceDiscovery;
  }

  @Override
  public String toString() {
    return String.format("flowlet=%s, instance=%d, groupsize=%s, %s",
                         getFlowletId(), getInstanceId(), getInstanceCount(), super.toString());
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  @Override
  public String getName() {
    return getFlowletId();
  }

  @Override
  public FlowletSpecification getSpecification() {
    return flowletSpec;
  }

  /**
   * @return A map of runtime key and value arguments supplied by the user.
   */
  @Override
  public Map<String, String> getRuntimeArguments() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : runtimeArguments) {
      builder.put(entry);
    }
    return builder.build();
  }

  @Override
  public ServiceDiscovered discover(String appId, String serviceId, String serviceName) {
    return serviceDiscovery.discover(accountId, appId, serviceId, serviceName);
  }

  public MetricsCollector getSystemMetrics() {
    return systemMetricsCollector;
  }

  public void setInstanceCount(int count) {
    instanceCount = count;
  }

  public String getFlowId() {
    return flowId;
  }

  public String getFlowletId() {
    return flowletId;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  public LoggingContext getLoggingContext() {
    return new FlowletLoggingContext(getAccountId(), getApplicationId(), getFlowId(), getFlowletId());
  }

  @Override
  public Metrics getMetrics() {
    return flowletMetrics;
  }

  public long getGroupId() {
    return groupId;
  }

  public String getMetricContext() {
    return String.format("%s.f.%s.%s.%d",
                         getApplicationId(),
                         getFlowId(),
                         getFlowletId(),
                         getInstanceId());
  }
}
