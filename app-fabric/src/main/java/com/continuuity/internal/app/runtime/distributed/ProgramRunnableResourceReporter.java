package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.internal.app.program.TypeId;
import com.continuuity.internal.app.runtime.AbstractResourceReporter;
import org.apache.twill.api.TwillContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports resource metrics about the runnable program.
 */
public class ProgramRunnableResourceReporter extends AbstractResourceReporter {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunnableResourceReporter.class);
  private final TwillContext runContext;
  private final String metricContext;

  public ProgramRunnableResourceReporter(Program program, MetricsCollectionService collectionService,
                                         TwillContext context) {
    super(collectionService);
    this.runContext = context;
    this.metricContext = getMetricContext(program, context);
  }

  @Override
  public void reportResources() {
    sendMetrics(metricContext, 1, runContext.getMaxMemoryMB(), runContext.getVirtualCores());
  }

  /**
   * Returns the metric context.  A metric context is of the form
   * {applicationId}.{programTypeId}.{programId}.{componentId}.  So for flows, it will look like
   * appX.f.flowY.flowletZ.  For procedures, appX.p.procedureY.  For mapreduce jobs, appX.b.mapredY.{optional m|r}.
   */
  private String getMetricContext(Program program, TwillContext context) {
    String metricContext = program.getApplicationId() + "." + TypeId.getMetricContextId(program.getType());
    if (program.getType() == Type.FLOW) {
      metricContext += "." + program.getName();
    }
    metricContext += "." + context.getSpecification().getName() + "." + context.getInstanceId();
    return metricContext;
  }
}
