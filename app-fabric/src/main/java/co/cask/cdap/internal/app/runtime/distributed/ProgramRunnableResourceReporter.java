/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.app.program.Program;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.internal.app.program.TypeId;
import co.cask.cdap.internal.app.runtime.AbstractResourceReporter;
import co.cask.cdap.proto.ProgramType;
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
    if (program.getType() == ProgramType.FLOW) {
      metricContext += "." + program.getName();
    }
    metricContext += "." + context.getSpecification().getName() + "." + context.getInstanceId();
    return metricContext;
  }
}
