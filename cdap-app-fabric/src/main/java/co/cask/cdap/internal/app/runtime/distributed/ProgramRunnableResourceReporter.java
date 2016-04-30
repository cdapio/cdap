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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.program.ProgramTypeMetricTag;
import co.cask.cdap.internal.app.runtime.AbstractResourceReporter;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.TwillContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Reports resource metrics about the runnable program.
 */
final class ProgramRunnableResourceReporter extends AbstractResourceReporter {
  private final TwillContext runContext;

  ProgramRunnableResourceReporter(ProgramId programId,
                                  MetricsCollectionService collectionService, TwillContext context) {
    super(collectionService.getContext(getMetricContext(programId, context)));
    this.runContext = context;
  }

  @Override
  public void reportResources() {
    sendMetrics(new HashMap<String, String>(), 1, runContext.getMaxMemoryMB(), runContext.getVirtualCores());
  }

  /**
   * Returns the metric context.  A metric context is of the form
   * {applicationId}.{programTypeId}.{programId}.{componentId}.  So for flows, it will look like
   * appX.f.flowY.flowletZ. For mapreduce jobs, appX.b.mapredY.{optional m|r}.
   */
  private static Map<String, String> getMetricContext(ProgramId programId, TwillContext context) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
      .put(Constants.Metrics.Tag.NAMESPACE, programId.getNamespace())
      .put(Constants.Metrics.Tag.RUN_ID, context.getRunId().getId())
      .put(Constants.Metrics.Tag.APP, programId.getApplication());

    if (programId.getType() == ProgramType.FLOW) {
      builder.put(Constants.Metrics.Tag.FLOW, programId.getProgram());
      builder.put(Constants.Metrics.Tag.FLOWLET, context.getSpecification().getName());
    } else {
      builder.put(ProgramTypeMetricTag.getTagName(programId.getType()), context.getSpecification().getName());
    }

    return builder.build();
  }
}
