package com.continuuity.internal.app.runtime.flow;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.logging.FlowletLoggingContext;
import com.continuuity.app.metrics.FlowletMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Internal implementation of {@link FlowletContext}.
 */
final class BasicFlowletContext implements FlowletContext {

  private final String accountId;
  private final String applicationId;
  private final String flowId;
  private final String flowletId;
  private final RunId runId;
  private final int instanceId;
  private final Map<String, DataSet> datasets;
  private final FlowletSpecification flowletSpec;
  private final CMetrics systemMetrics;

  private volatile int instanceCount;
  private final QueueProducer queueProducer;
  private volatile QueueConsumer queueConsumer;
  private final boolean asyncMode;
  private FlowletMetrics flowletMetrics;

  BasicFlowletContext(Program program, String flowletId, int instanceId,
                      Map<String, DataSet> datasets, FlowletSpecification flowletSpec, boolean asyncMode) {
    this.accountId = program.getAccountId();
    this.applicationId = program.getApplicationId();
    this.flowId = program.getProgramName();
    this.flowletId = flowletId;
    this.runId = RunId.generate();
    this.instanceId = instanceId;
    this.datasets = ImmutableMap.copyOf(datasets);
    this.flowletSpec = flowletSpec;

    this.instanceCount = program.getSpecification().getFlows().get(flowId).getFlowlets().get(flowletId).getInstances();
    this.queueProducer = new QueueProducer(getMetricName());
    this.queueConsumer = createQueueConsumer();
    this.asyncMode = asyncMode;

    this.flowletMetrics = new FlowletMetrics(accountId, applicationId, flowId, flowletId, runId.toString(), instanceId);
    this.systemMetrics = new CMetrics(MetricType.FlowSystem, getMetricName());
  }

  @Override
  public String toString() {
    return String.format("flowlet=%s, instance=%d, groupsize=%s, runid=%s",
                         getFlowletId(), getInstanceId(), getInstanceCount(), getRunId());
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
  public <T extends DataSet> T getDataSet(String name) {
    T dataSet = (T) datasets.get(name);
    Preconditions.checkArgument(dataSet != null, "%s is not a known DataSet.", name);
    return dataSet;
  }

  @Override
  public FlowletSpecification getSpecification() {
    return flowletSpec;
  }

  public void setInstanceCount(int count) {
    instanceCount = count;
    queueConsumer = createQueueConsumer();
  }

  public boolean isAsyncMode() {
    return asyncMode;
  }

  public String getAccountId() {
    return accountId;
  }

  public String getApplicationId() {
    return applicationId;
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

  public RunId getRunId() {
    return runId;
  }

  public QueueProducer getQueueProducer() {
    return queueProducer;
  }

  public QueueConsumer getQueueConsumer() {
    return queueConsumer;
  }

  public LoggingContext getLoggingContext() {
    return new FlowletLoggingContext(getAccountId(), getApplicationId(), getFlowId(), getFlowletId());

  }

  public Metrics getMetrics() {
    return flowletMetrics;
  }

  public CMetrics getSystemMetrics() {
    return systemMetrics;
  }

  private QueueConsumer createQueueConsumer() {
    int groupId = 100000 + Objects.hashCode(getFlowletId(), getFlowletId());
    // TODO: Consumer partitioning
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, ! asyncMode);
    return new QueueConsumer(getInstanceId(), groupId, getInstanceCount(), getMetricName(), config);
  }

  private String getMetricName() {
    return String.format("%s.%s.%s.%s.%s.%d",
                         getAccountId(),
                         getApplicationId(),
                         getFlowId(),
                         getRunId(),
                         getFlowletId(),
                         getInstanceId());
  }
}
