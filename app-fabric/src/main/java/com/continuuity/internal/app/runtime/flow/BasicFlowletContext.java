package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.data.DataSet;
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
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.continuuity.internal.app.runtime.AbstractContext;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.continuuity.weave.api.RunId;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.Map;

/**
 * Internal implementation of {@link FlowletContext}.
 */
final class BasicFlowletContext extends AbstractContext implements FlowletContext {

  private final String flowId;
  private final String flowletId;
  private final int instanceId;
  private final FlowletSpecification flowletSpec;

  private volatile int instanceCount;
  private final QueueProducer queueProducer;
  private final boolean asyncMode;
  private final FlowletMetrics flowletMetrics;
  private final Arguments runtimeArguments;

  private final MetricsCollector systemMetricsCollector;

  BasicFlowletContext(Program program, String flowletId, int instanceId, RunId runId, int instanceCount,
                      Map<String, DataSet> datasets, Arguments runtimeArguments,
                      FlowletSpecification flowletSpec, boolean asyncMode,
                      MetricsCollectionService metricsCollectionService) {
    super(program, runId, datasets);
    this.flowId = program.getProgramName();
    this.flowletId = flowletId;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.runtimeArguments = runtimeArguments;
    this.flowletSpec = flowletSpec;
    this.asyncMode = asyncMode;

    this.instanceCount = program.getSpecification().getFlows().get(flowId).getFlowlets().get(flowletId).getInstances();
    this.queueProducer = new QueueProducer(getMetricContext());

    this.flowletMetrics = new FlowletMetrics(metricsCollectionService, getApplicationId(), flowId, flowletId);
    this.systemMetricsCollector = getMetricsCollector(MetricsScope.REACTOR,
                                                      metricsCollectionService, getMetricContext());
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
    ImmutableMap.Builder<String, String> arguments = ImmutableMap.builder();
    Iterator<Map.Entry<String, String>> it = runtimeArguments.iterator();
    while (it.hasNext()) {
      arguments.put(it.next());
    }
    return arguments.build();
  }

  public MetricsCollector getSystemMetrics() {
    return systemMetricsCollector;
  }

  public void setInstanceCount(int count) {
    instanceCount = count;
  }

  public boolean isAsyncMode() {
    return asyncMode;
  }

  public String getFlowId() {
    return flowId;
  }

  public String getFlowletId() {
    return flowletId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public QueueProducer getQueueProducer() {
    return queueProducer;
  }

  public LoggingContext getLoggingContext() {
    return new FlowletLoggingContext(getAccountId(), getApplicationId(), getFlowId(), getFlowletId());
  }

  public Metrics getMetrics() {
    return flowletMetrics;
  }

  public long getGroupId() {
    int gid = 100000 + Objects.hashCode(getAccountId(), getApplicationId(), getFlowId(), getFlowletId());
    return   0xffffffffL & gid;
  }

  public String getMetricContext() {
    return String.format("%s.f.%s.%s.%d",
                         getApplicationId(),
                         getFlowId(),
                         getFlowletId(),
                         getInstanceId());
  }
}
