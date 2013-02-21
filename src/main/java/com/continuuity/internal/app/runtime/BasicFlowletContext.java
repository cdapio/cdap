package com.continuuity.internal.app.runtime;

import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.app.logging.FlowletLoggingContext;
import com.continuuity.app.metrics.FlowletMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueuePartitioner;
import com.continuuity.data.operation.ttqueue.QueueProducer;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class BasicFlowletContext implements FlowletContext {

  private final String accountId;
  private final String applicationId;
  private final String flowId;
  private final String flowletId;
  private final RunId runId;
  private final int instanceId;
  private final Map<String, DataSet> datasets;

  private final AtomicInteger instanceCount;
  private final boolean asyncMode;

  public BasicFlowletContext(Program program, String flowletId, int instanceId, Map<String, DataSet> datasets) {
    this(program, flowletId, instanceId, datasets, false);
  }

  public BasicFlowletContext(Program program, String flowletId, int instanceId,
                             Map<String, DataSet> datasets, boolean asyncMode) {
    this.accountId = program.getAccountId();
    this.applicationId = program.getApplicationId();
    this.flowId = program.getProgramName();
    this.flowletId = flowletId;
    this.runId = RunId.generate();
    this.instanceId = instanceId;
    this.datasets = ImmutableMap.copyOf(datasets);
    this.instanceCount = new AtomicInteger(program.getSpecification().getFlows().get(flowId)
                                             .getFlowlets().get(flowletId).getInstances());
    this.asyncMode = asyncMode;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount.get();
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

  public void setInstanceCount(int count) {
    instanceCount.set(count);
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
    return new QueueProducer(getMetricName());
  }

  public QueueConsumer getQueueConsumer() {
    int groupId = 100000 + Objects.hashCode(getFlowletId(), getFlowletId());
    // TODO: Consumer partitioning
    QueueConfig config = new QueueConfig(QueuePartitioner.PartitionerType.FIFO, ! asyncMode);
    return new QueueConsumer(getInstanceId(), groupId, getInstanceCount(), getMetricName(), config);
  }

  public LoggingContext getLoggingContext() {
    return new FlowletLoggingContext(getAccountId(), getApplicationId(), getFlowId(), getFlowletId());

  }

  public Metrics getMetrics() {
    return new FlowletMetrics(getAccountId(), getApplicationId(), getFlowId(),
                              getFlowletId(), getRunId().toString(), getInstanceId());
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
