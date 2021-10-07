/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.metrics.ProgramTypeMetricTag;
import io.cdap.cdap.internal.app.runtime.AbstractResourceReporter;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.api.TwillContext;

import java.util.Collections;
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
    sendMetrics(Collections.emptyMap(), 1, runContext.getMaxMemoryMB(), runContext.getVirtualCores());
  }

  /**
   * Returns the metric context.  A metric context is of the form
   * {applicationId}.{programTypeId}.{programId}.{componentId}.  So for flows, it will look like
   * appX.f.flowY.flowletZ. For mapreduce jobs, appX.b.mapredY.{optional m|r}.
   */
  private static Map<String, String> getMetricContext(ProgramId programId, TwillContext context) {
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, programId.getNamespace());
    tags.put(Constants.Metrics.Tag.RUN_ID, context.getRunId().getId());
    tags.put(Constants.Metrics.Tag.APP, programId.getApplication());

    tags.put(ProgramTypeMetricTag.getTagName(programId.getType()), programId.getProgram());

    return tags;
  }
}
